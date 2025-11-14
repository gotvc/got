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
	BranchEntry = gotnsop.BranchEntry
	VolumeEntry = gotnsop.VolumeEntry
)

func (m *Machine) GetBranch(ctx context.Context, s stores.Reading, state State, name string) (*gotnsop.BranchEntry, error) {
	val, err := m.gotkv.Get(ctx, s, state.Branches, []byte(name))
	if err != nil {
		if gotkv.IsErrKeyNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	entry, err := gotnsop.ParseBranchEntry([]byte(name), val)
	if err != nil {
		return nil, err
	}
	return &entry, nil
}

func (m *Machine) PutBranch(ctx context.Context, s stores.RW, State State, entry gotnsop.BranchEntry) (*State, error) {
	mut1 := putBranch(entry)
	entsState, err := m.gotkv.Mutate(ctx, s, State.Branches, mut1)
	if err != nil {
		return nil, err
	}
	State.Branches = *entsState
	return &State, nil
}

func (m *Machine) DeleteBranch(ctx context.Context, s stores.RW, State State, name string) (*State, error) {
	entsState, err := m.gotkv.Delete(ctx, s, State.Branches, []byte(name))
	if err != nil {
		return nil, err
	}
	State.Branches = *entsState
	return &State, nil
}

func putBranch(entry gotnsop.BranchEntry) gotkv.Mutation {
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

func (m *Machine) ListBranches(ctx context.Context, s stores.Reading, state State, span branches.Span, limit int) ([]gotnsop.BranchEntry, error) {
	span2 := gotkv.TotalSpan()
	if span.Begin != "" {
		span2.Begin = []byte(span.Begin)
	}
	if span.End != "" {
		span2.End = []byte(span.End)
	}
	it := m.gotkv.NewIterator(s, state.Branches, span2)
	var ents []gotnsop.BranchEntry
	for {
		ent, err := streams.Next(ctx, it)
		if err != nil {
			if streams.IsEOS(err) {
				break
			}
			return nil, err
		}

		entry, err := gotnsop.ParseBranchEntry(ent.Key, ent.Value)
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
	if entry.HashOfSecret == ([32]byte{}) {
		return nil, fmt.Errorf("hash of secret must be non-zero for volumes")
	}
	tx := m.gotkv.NewTx(s, state.Volumes)
	var val []byte
	if found, err := tx.Get(ctx, entry.Key(nil), &val); err != nil {
		return nil, err
	} else if found {
		return nil, fmt.Errorf("volume %v is already in this namespace", entry.Volume)
	}
	if err := tx.Put(ctx, entry.Key(nil), entry.Value(nil)); err != nil {
		return nil, err
	}
	nextVol, err := tx.Finish(ctx)
	if err != nil {
		return nil, err
	}
	state.Volumes = *nextVol
	return &state, nil
}

func (m *Machine) GetVolume(ctx context.Context, s stores.Reading, state State, volOID blobcache.OID) (*VolumeEntry, error) {
	val, err := m.gotkv.Get(ctx, s, state.Volumes, volOID[:])
	if err != nil {
		return nil, err
	}
	entry, err := gotnsop.ParseVolumeEntry(volOID[:], val)
	if err != nil {
		return nil, err
	}
	return entry, nil
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
