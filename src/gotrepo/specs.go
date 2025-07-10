package gotrepo

import (
	"context"
	"encoding/json"
	"fmt"

	"go.brendoncarroll.net/p2p"
	"go.brendoncarroll.net/state/cells/httpcell"
	"go.inet256.org/inet256/pkg/inet256"

	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/gotgrpc"
	"github.com/gotvc/got/src/internal/cells"
)

type StoreSpec struct {
	Local *LocalStoreSpec `json:"local,omitempty"`
	// Blobcache *BlobcacheStoreSpec `json:"blobcache,omitempty"`
}

type LocalStoreSpec = StoreID

// type BlobcacheStoreSpec = blobcache.PinSetHandle

func (r *Repo) MakeStore(spec StoreSpec) (Store, error) {
	switch {
	case spec.Local != nil:
		return r.storeManager.Open(*spec.Local), nil
	default:
		return nil, fmt.Errorf("empty store spec")
	}
}

type BranchSpec struct {
	Volume VolumeSpec `json:"volume"`
	branches.Info
}

type VolumeSpec struct {
	Cell     CellSpec  `json:"cell"`
	VCStore  StoreSpec `json:"vc_store"`
	FSStore  StoreSpec `json:"fs_store"`
	RawStore StoreSpec `json:"raw_store"`
}

func ParseVolumeSpec(data []byte) (*VolumeSpec, error) {
	var spec VolumeSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, err
	}
	return &spec, nil
}

func (r *Repo) MakeVolume(spec VolumeSpec) (*Volume, error) {
	cell, err := r.MakeCell(spec.Cell)
	if err != nil {
		return nil, err
	}
	vcStore, err := r.MakeStore(spec.VCStore)
	if err != nil {
		return nil, err
	}
	fsStore, err := r.MakeStore(spec.VCStore)
	if err != nil {
		return nil, err
	}
	rawStore, err := r.MakeStore(spec.VCStore)
	if err != nil {
		return nil, err
	}
	return &Volume{
		Cell:     cell,
		VCStore:  vcStore,
		FSStore:  fsStore,
		RawStore: rawStore,
	}, nil
}

type CellSpec struct {
	Local     *LocalCellSpec     `json:"local,omitempty"`
	HTTP      *HTTPCellSpec      `json:"http,omitempty"`
	Encrypted *EncryptedCellSpec `json:"encrypted,omitempty"`
}

type LocalCellSpec = CellID

type HTTPCellSpec = httpcell.Spec

type EncryptedCellSpec struct {
	Inner  CellSpec `json:"inner"`
	Secret []byte   `json:"secret"`
}

type PeerCellSpec struct {
	ID   p2p.PeerID `json:"id"`
	Name string     `json:"name"`
}

func ParseCellSpec(data []byte) (*CellSpec, error) {
	cellSpec := CellSpec{}
	if err := json.Unmarshal(data, &cellSpec); err != nil {
		return nil, err
	}
	return &cellSpec, nil
}

func (r *Repo) MakeCell(spec CellSpec) (Cell, error) {
	switch {
	case spec.Local != nil:
		return r.cellManager.Open(*spec.Local), nil

	case spec.HTTP != nil:
		return httpcell.New(*spec.HTTP), nil

	case spec.Encrypted != nil:
		inner, err := r.MakeCell(spec.Encrypted.Inner)
		if err != nil {
			return nil, err
		}
		if len(spec.Encrypted.Secret) != 32 {
			return nil, fmt.Errorf("encrypted cell has incorrect secret length. HAVE: %d WANT: %d", len(spec.Encrypted.Secret), 32)
		}
		secret := (*[32]byte)(spec.Encrypted.Secret)
		return cells.NewEncrypted(inner, secret), nil

	default:
		return nil, fmt.Errorf("empty cell spec")
	}
}

type MultiSpaceSpec []SpaceLayerSpec

type SpaceLayerSpec struct {
	Prefix string    `json:"prefix"`
	Target SpaceSpec `json:"target"`
}

type QUICSpaceSpec struct {
	ID   inet256.ID `json:"id"`
	Addr string     `json:"addr"`
}

type GRPCSpaceSpec struct {
	Endpoint string            `json:"endpoint"`
	Headers  map[string]string `json:"headers,omitempty"`
}

type CryptoSpaceSpec struct {
	Inner       SpaceSpec `json:"inner"`
	Secret      []byte    `json:"secret"`
	Passthrough []string  `json:"passthrough,omitempty"`
}

type PrefixSpaceSpec struct {
	Inner  SpaceSpec `json:"inner"`
	Prefix string    `json:"prefix"`
}

type SpaceSpec struct {
	Peer *inet256.ID    `json:"peer,omitempty"`
	QUIC *QUICSpaceSpec `json:"quic,omitempty"`
	GRPC *GRPCSpaceSpec `json:"grpc,omitempty"`

	Crypto *CryptoSpaceSpec `json:"crypto,omitempty"`
	Prefix *PrefixSpaceSpec `json:"prefix,omitempty"`
}

func (r *Repo) MakeSpace(spec SpaceSpec) (Space, error) {
	switch {
	case spec.Peer != nil:
		gn, err := r.getGotNet()
		if err != nil {
			return nil, err
		}
		return gn.GetSpace(*spec.Peer), nil
	case spec.QUIC != nil:
		gn, err := r.getQUICGotNet(*spec.QUIC)
		if err != nil {
			return nil, err
		}
		return gn.GetSpace(spec.QUIC.ID), nil
	case spec.GRPC != nil:
		c, err := r.getGRPCClient(spec.GRPC.Endpoint, spec.GRPC.Headers)
		if err != nil {
			return nil, err
		}
		return gotgrpc.NewSpace(c), nil
	case spec.Crypto != nil:
		secret := spec.Crypto.Secret
		if len(secret) != branches.SecretSize {
			return nil, fmt.Errorf("crypto secret key is wrong size. HAVE: %d WANT: %d", len(secret), branches.SecretSize)
		}
		innerSpec := spec.Crypto.Inner
		inner, err := r.MakeSpace(innerSpec)
		if err != nil {
			return nil, err
		}
		var opts []branches.CryptoSpaceOption
		if spec.Crypto.Passthrough != nil {
			opts = append(opts, branches.WithPassthrough(spec.Crypto.Passthrough))
		}
		return branches.NewCryptoSpace(inner, (*[32]byte)(secret), opts...), nil
	case spec.Prefix != nil:
		inner, err := r.MakeSpace(spec.Prefix.Inner)
		if err != nil {
			return nil, err
		}
		return branches.NewPrefixSpace(inner, spec.Prefix.Prefix), nil
	default:
		return nil, fmt.Errorf("empty SpaceSpec")
	}
}

func (r *Repo) spaceFromSpecs(specs []SpaceLayerSpec) (branches.Space, error) {
	var layers []branches.Layer
	for _, spec := range specs {
		layer := branches.Layer{
			Prefix: spec.Prefix,
			Target: newLazySpace(func(ctx context.Context) (branches.Space, error) {
				return r.MakeSpace(spec.Target)
			}),
		}
		layers = append(layers, layer)
	}
	layers = append(layers, branches.Layer{
		Prefix: "",
		Target: r.specDir,
	})
	return branches.NewMultiSpace(layers)
}
