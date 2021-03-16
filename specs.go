package got

import (
	"encoding/json"

	"github.com/blobcache/blobcache/pkg/blobcache"
	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/c/httpcell"
	"github.com/brendoncarroll/got/pkg/cells"
	"github.com/brendoncarroll/got/pkg/gotnet"
	"github.com/pkg/errors"
)

type StoreSpec struct {
	Local     *LocalStoreSpec     `json:"local,omitempty"`
	Blobcache *BlobcacheStoreSpec `json:"blobcache,omitempty"`
	Peer      *PeerStoreSpec      `json:"peer,omitempty"`
}

type LocalStoreSpec struct{}

type BlobcacheStoreSpec struct {
	Addr     string             `json:"addr"`
	PinSetID blobcache.PinSetID `json:"pinset_id"`
}

func DefaultBlobcacheSpec() StoreSpec {
	return StoreSpec{
		Blobcache: &BlobcacheStoreSpec{
			Addr: "127.0.0.1:6025",
		},
	}
}

type PeerStoreSpec struct {
	ID p2p.PeerID `json:"id"`
}

func (r *Repo) MakeStore(spec StoreSpec) (Store, error) {
	switch {
	case spec.Local != nil:
		return blobs.NewMem(), nil // TODO: persistant
	default:
		return nil, errors.Errorf("empty store spec")
	}
}

type EnvSpec struct {
	Cell  CellSpec  `json:"cell"`
	Store StoreSpec `json:"store"`
}

func (r *Repo) MakeEnv(k string, spec EnvSpec) (*Env, error) {
	cell, err := r.MakeCell(k, spec.Cell)
	if err != nil {
		return nil, err
	}
	store, err := r.MakeStore(spec.Store)
	if err != nil {
		return nil, err
	}
	return &Env{Cell: cell, Store: store}, nil
}

type CellSpec struct {
	Local     *LocalCellSpec     `json:"local"`
	HTTP      *HTTPCellSpec      `json:"http"`
	SecretBox *SecretBoxCellSpec `json:"secretbox"`
	Peer      *PeerCellSpec      `json:"peer"`
	Signed    *SignedCellSpec    `json:"signed"`
}

type LocalCellSpec struct{}

type HTTPCellSpec = httpcell.Spec

type SecretBoxCellSpec struct {
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

func (r *Repo) MakeCell(k string, spec CellSpec) (Cell, error) {
	switch {
	case spec.Local != nil:
		return newBoltCell(r.db, k), nil

	case spec.HTTP != nil:
		return httpcell.New(*spec.HTTP), nil

	case spec.SecretBox != nil:
		inner, err := r.MakeCell(k, spec.SecretBox.Inner)
		if err != nil {
			return nil, err
		}
		return cells.NewSecretBox(inner, spec.SecretBox.Secret), nil

	case spec.Peer != nil:
		panic("not implemented")

	case spec.Signed != nil:
		inner, err := r.MakeCell(k, spec.Signed.Inner)
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
		return cells.NewSigned(inner, "got-signed-cell", pubKey, privateKey), nil

	default:
		return nil, errors.Errorf("empty cell spec")
	}
}

type RealmSpec struct {
	Peer *PeerCellSpaceSpec
}

type PeerCellSpaceSpec struct {
	ID p2p.PeerID `json:"id"`
}

func (r *Repo) MakeRealm(spec RealmSpec) (Realm, error) {
	switch {
	case spec.Peer != nil:
		swarm, err := r.getSwarm()
		if err != nil {
			return nil, err
		}
		return gotnet.NewClient(swarm, spec.Peer.ID), nil
	default:
		return nil, errors.Errorf("empty cell space spec")
	}
}
