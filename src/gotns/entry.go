package gotns

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/exp/streams"

	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/gotns/internal/gotnsop"
	"github.com/gotvc/got/src/internal/stores"
)

type (
	AliasEntry  = gotnsop.AliasEntry
	VolumeEntry = gotnsop.VolumeEntry
)

// GetAlias looks up an entry in the branches table.
func (m *Machine) GetAlias(ctx context.Context, s stores.Reading, state State, name string) (*gotnsop.AliasEntry, error) {
	val, err := m.gotkv.Get(ctx, s, state.Aliases, []byte(name))
	if err != nil {
		if gotkv.IsErrKeyNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	entry, err := gotnsop.ParseAliasEntry([]byte(name), val)
	if err != nil {
		return nil, err
	}
	return &entry, nil
}

// PutAlias inserts or overwrites an entry in the branches table.
func (m *Machine) PutAlias(ctx context.Context, s stores.RW, state State, entry gotnsop.AliasEntry, secret *gotnsop.Secret) (*State, error) {
	mut1 := putAlias(entry)
	aliasState, err := m.gotkv.Mutate(ctx, s, state.Aliases, mut1)
	if err != nil {
		return nil, err
	}
	state.Aliases = *aliasState
	nextState, err := m.FulfillObligations(ctx, s, state, entry.Name, secret)
	if err != nil {
		return nil, err
	}
	return nextState, nil
}

// DeleteAlias deletes an alias from the namespace.
func (m *Machine) DeleteAlias(ctx context.Context, s stores.RW, State State, name string) (*State, error) {
	entsState, err := m.gotkv.Delete(ctx, s, State.Aliases, []byte(name))
	if err != nil {
		return nil, err
	}
	State.Aliases = *entsState
	return &State, nil
}

func putAlias(entry gotnsop.AliasEntry) gotkv.Mutation {
	k := entry.Key(nil)
	return gotkv.Mutation{
		Span: gotkv.SingleKeySpan(k),
		Entries: []gotkv.Entry{
			{
				Key:   entry.Key(nil),
				Value: entry.Value(nil),
			},
		},
	}
}

func (m *Machine) ListBranches(ctx context.Context, s stores.Reading, state State, span branches.Span, limit int) ([]gotnsop.AliasEntry, error) {
	span2 := gotkv.TotalSpan()
	if span.Begin != "" {
		span2.Begin = []byte(span.Begin)
	}
	if span.End != "" {
		span2.End = []byte(span.End)
	}
	it := m.gotkv.NewIterator(s, state.Aliases, span2)
	var ents []gotnsop.AliasEntry
	for {
		ent, err := streams.Next(ctx, it)
		if err != nil {
			if streams.IsEOS(err) {
				break
			}
			return nil, err
		}

		entry, err := gotnsop.ParseAliasEntry(ent.Key, ent.Value)
		if err != nil {
			return nil, err
		}
		ents = append(ents, entry)
		if limit > 0 && len(ents) >= limit {
			break
		}
	}
	return ents, nil
}

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
