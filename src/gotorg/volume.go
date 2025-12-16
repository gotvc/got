package gotorg

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/cloudflare/circl/sign"
	"go.inet256.org/inet256/src/inet256"

	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/gotorg/internal/gotorgop"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/volumes"
)

// AddVolume adds a new Volume to the namespace.
// It's OID must be unique within the namespace or an error will be returned.
func (m *Machine) AddVolume(ctx context.Context, s stores.RW, state State, entry VolumeEntry) (*State, error) {
	if len(entry.HashOfSecrets) == 0 {
		return nil, fmt.Errorf("hash of secret must be non-zero for volumes")
	}
	if err := m.mapKV(ctx, s, &state.Volumes, func(tx *gotkv.Tx) error {
		var val []byte
		if found, err := tx.Get(ctx, entry.Key(nil), &val); err != nil {
			return err
		} else if found {
			return fmt.Errorf("volume %v is already in this namespace", entry.Volume)
		}
		if err := tx.Put(ctx, entry.Key(nil), entry.Value(nil)); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &state, nil
}

// GetVolume looks up a volume in the volumes table.
// If the volume is not found, nil is returned.
func (m *Machine) GetVolume(ctx context.Context, s stores.Reading, state State, volOID blobcache.OID) (*VolumeEntry, error) {
	val, err := m.gotkv.Get(ctx, s, state.Volumes, volOID[:])
	if gotkv.IsErrKeyNotFound(err) {
		return nil, nil
	}
	return gotorgop.ParseVolumeEntry(volOID[:], val)
}

func (m *Machine) DropVolume(ctx context.Context, s stores.RW, state State, volOID blobcache.OID) (*State, error) {
	next, err := m.gotkv.Delete(ctx, s, state.Volumes, volOID[:])
	if err != nil {
		return nil, err
	}
	state.Volumes = *next
	return &state, nil
}

func (m *Machine) ForEachVolume(ctx context.Context, s stores.Reading, x State, fn func(entry VolumeEntry) error) error {
	span := gotkv.TotalSpan()
	return m.gotkv.ForEach(ctx, s, x.Volumes, span, func(ent gotkv.Entry) error {
		entry, err := gotorgop.ParseVolumeEntry(ent.Key, ent.Value)
		if err != nil {
			return err
		}
		return fn(*entry)
	})
}

type VolumeConstructor = func(nsVol, innerVol branches.Volume) *Volume

func (m *Machine) Open(ctx context.Context, s stores.Reading, x State, actAs IdenPrivate, volid blobcache.OID, writeAccess bool) (VolumeConstructor, error) {
	vent, err := m.GetVolume(ctx, s, x, volid)
	if err != nil {
		return nil, err
	}
	minRatchet := uint8(1)
	if writeAccess {
		minRatchet = 2
	}
	secret, ratchet, err := m.FindSecret(ctx, s, x, actAs, &vent.HashOfSecrets[0], minRatchet)
	if err != nil {
		return nil, err
	}
	rs := secret.Ratchet(int(ratchet) - 1).DeriveSym()
	symSecret := &rs

	mkVol := func(nsVol, innerVol branches.Volume) *Volume {
		return newVolume(m, actAs.SigPrivateKey, nsVol, innerVol, symSecret)
	}
	return mkVol, nil
}

func (m *Machine) OpenAt(ctx context.Context, s stores.Reading, x State, actAs IdenPrivate, name string, writeAccess bool) (blobcache.OID, VolumeConstructor, error) {
	ent, err := m.GetAlias(ctx, s, x, name)
	if err != nil {
		return blobcache.OID{}, nil, err
	}
	if ent == nil {
		return blobcache.OID{}, nil, branches.ErrNotExist
	}
	vw, err := m.Open(ctx, s, x, actAs, ent.Volume, writeAccess)
	if err != nil {
		return blobcache.OID{}, nil, err
	}
	return ent.Volume, vw, nil
}

var _ volumes.Volume = &Volume{}

type Volume struct {
	m        *Machine
	nsVol    volumes.Volume
	innerVol volumes.Volume

	privateKey sign.PrivateKey
	symSecret  *[32]byte
}

func newVolume(m *Machine, privKey sign.PrivateKey, nsVol, innerVol volumes.Volume, symSecret *[32]byte) *Volume {
	return &Volume{
		m:        m,
		nsVol:    nsVol,
		innerVol: innerVol,

		privateKey: privKey,
		symSecret:  symSecret,
	}
}

func (v *Volume) BeginTx(ctx context.Context, txp blobcache.TxParams) (volumes.Tx, error) {
	symVol := volumes.NewChaCha20Poly1305(v.innerVol, v.symSecret)
	sigVol := volumes.NewSignedVolume(symVol, pki, v.privateKey, v.getVerifier)
	return sigVol.BeginTx(ctx, txp)
}

func (v *Volume) getVerifier(ctx context.Context, id inet256.ID) (sign.PublicKey, error) {
	tx, err := v.nsVol.BeginTx(ctx, blobcache.TxParams{})
	if err != nil {
		return nil, err
	}
	defer tx.Abort(ctx)
	x, err := loadState(ctx, tx)
	if err != nil {
		return nil, err
	}
	idu, err := v.m.GetIDUnit(ctx, tx, *x, id)
	if err != nil {
		return nil, err
	}
	if idu == nil {
		return nil, fmt.Errorf("could not find verifier")
	}
	return idu.SigPublicKey, nil
}

type loader interface {
	Load(ctx context.Context, dst *[]byte) error
}

func loadState(ctx context.Context, ldr loader) (*State, error) {
	var buf []byte
	if err := ldr.Load(ctx, &buf); err != nil {
		return nil, err
	}
	r, err := Parse(buf)
	if err != nil {
		return nil, err
	}
	return &r.State.Current, nil
}
