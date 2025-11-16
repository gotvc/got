package gotns

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/cloudflare/circl/sign"

	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/gotns/internal/gotnsop"
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
	return gotnsop.ParseVolumeEntry(volOID[:], val)
}

func (m *Machine) DropVolume(ctx context.Context, s stores.RW, state State, volOID blobcache.OID) (*State, error) {
	next, err := m.gotkv.Delete(ctx, s, state.Volumes, volOID[:])
	if err != nil {
		return nil, err
	}
	state.Volumes = *next
	return &state, nil
}

func (m *Machine) ForEachVolume(ctx context.Context, s stores.Reading, state State, fn func(entry VolumeEntry) error) error {
	span := gotkv.TotalSpan()
	return m.gotkv.ForEach(ctx, s, state.Volumes, span, func(ent gotkv.Entry) error {
		entry, err := gotnsop.ParseVolumeEntry(ent.Key, ent.Value)
		if err != nil {
			return err
		}
		return fn(*entry)
	})
}

func newVolume(inner volumes.Volume, readSecret *[32]byte, privateKey sign.PrivateKey, getVerifier volumes.GetVerifierFunc) volumes.Volume {
	symVol := volumes.NewChaCha20Poly1305(inner, readSecret)
	sigVol := volumes.NewSignedVolume(symVol, privateKey, getVerifier)
	return sigVol
}
