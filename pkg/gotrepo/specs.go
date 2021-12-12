package gotrepo

import (
	"encoding/json"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-state/cells/httpcell"
	"github.com/brendoncarroll/go-tai64"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/cells"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/pkg/errors"
)

type StoreSpec struct {
	Local     *LocalStoreSpec     `json:"local,omitempty"`
	Blobcache *BlobcacheStoreSpec `json:"blobcache,omitempty"`
}

type LocalStoreSpec = StoreID

type BlobcacheStoreSpec struct {
	Addr string `json:"addr"`
	// TODO: add back once blobcache is working
	// PinSetID blobcache.PinSetID `json:"pinset_id"`
}

func DefaultBlobcacheSpec() StoreSpec {
	return StoreSpec{
		Blobcache: &BlobcacheStoreSpec{
			Addr: "127.0.0.1:6025",
		},
	}
}

func (r *Repo) MakeStore(spec StoreSpec) (Store, error) {
	switch {
	case spec.Local != nil:
		return r.storeManager.GetStore(*spec.Local), nil
	default:
		return nil, errors.Errorf("empty store spec")
	}
}

type BranchSpec struct {
	Volume    VolumeSpec  `json:"volume"`
	Salt      []byte      `json:"salt"`
	CreatedAt tai64.TAI64 `json:"created_at"`
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
	Signed    *SignedCellSpec    `json:"signed,omitempty"`
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

type SignedCellSpec struct {
	Inner          CellSpec `json:"inner"`
	PublicKeyX509  []byte   `json:"public_key"`
	PrivateKeyX509 []byte   `json:"private_key"`
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
		return r.cellManager.Get(*spec.Local)

	case spec.HTTP != nil:
		return httpcell.New(*spec.HTTP), nil

	case spec.Encrypted != nil:
		inner, err := r.MakeCell(spec.Encrypted.Inner)
		if err != nil {
			return nil, err
		}
		return cells.NewEncrypted(inner, spec.Encrypted.Secret), nil

	case spec.Signed != nil:
		inner, err := r.MakeCell(spec.Signed.Inner)
		if err != nil {
			return nil, err
		}
		pubKey, err := p2p.ParsePublicKey(spec.Signed.PublicKeyX509)
		if err != nil {
			return nil, err
		}
		var privateKey p2p.PrivateKey
		if len(spec.Signed.PrivateKeyX509) > 0 {
			privKey, err := parsePrivateKey(spec.Signed.PrivateKeyX509)
			if err != nil {
				return nil, err
			}
			privateKey = privKey
		}
		return cells.NewSigned(inner, pubKey, privateKey), nil

	default:
		return nil, errors.Errorf("empty cell spec")
	}
}

type MultiSpaceSpec []SpaceLayerSpec

type SpaceLayerSpec struct {
	Prefix string    `json:"prefix"`
	Target SpaceSpec `json:"target"`
}

type SpaceSpec struct {
	Peer *inet256.ID `json:"peer,omitempty"`
}

func (r *Repo) MakeSpace(spec SpaceSpec) (Space, error) {
	switch {
	case spec.Peer != nil:
		gn, err := r.getGotNet()
		if err != nil {
			return nil, err
		}
		return gn.GetSpace(*spec.Peer), nil
	default:
		return nil, errors.Errorf("empty SpaceSpec")
	}
}

func (r *Repo) spaceFromSpecs(specs []SpaceLayerSpec) (branches.Space, error) {
	var layers []branches.Layer
	for _, spec := range specs {
		space, err := r.MakeSpace(spec.Target)
		if err != nil {
			return nil, err
		}
		layer := branches.Layer{
			Prefix: spec.Prefix,
			Target: space,
		}
		layers = append(layers, layer)
	}
	layers = append(layers, branches.Layer{
		Prefix: "",
		Target: r.specDir,
	})
	return branches.NewMultiSpace(layers)
}
