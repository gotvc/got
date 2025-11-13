package gotns

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/gotns/internal/gotnsop"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/streams"
)

type Entry = gotnsop.BranchEntry

// NewEntry creates a new entry with the provided information
// and produces KEMs for each group with access to the entry.
func (m *Machine) NewEntry(ctx context.Context, name string, rights blobcache.ActionSet, volume blobcache.OID, secret *[32]byte) (Entry, error) {
	entry := Entry{
		Name:   name,
		Volume: volume,
	}
	return entry, nil
}

func (m *Machine) GetEntry(ctx context.Context, s stores.Reading, State State, name []byte) (*Entry, error) {
	val, err := m.gotkv.Get(ctx, s, State.Branches, name)
	if err != nil {
		if gotkv.IsErrKeyNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	entry, err := gotnsop.ParseBranchEntry(name, val)
	if err != nil {
		return nil, err
	}
	return &entry, nil
}

func (m *Machine) PutEntry(ctx context.Context, s stores.RW, State State, entry Entry) (*State, error) {
	mut := PutEntry(entry)
	entsState, err := m.gotkv.Mutate(ctx, s, State.Branches, mut)
	if err != nil {
		return nil, err
	}
	State.Branches = *entsState
	return &State, nil
}

func (m *Machine) DeleteEntry(ctx context.Context, s stores.RW, State State, name string) (*State, error) {
	entsState, err := m.gotkv.Delete(ctx, s, State.Branches, []byte(name))
	if err != nil {
		return nil, err
	}
	State.Branches = *entsState
	return &State, nil
}

func PutEntry(entry Entry) gotkv.Mutation {
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

func (m *Machine) ListEntries(ctx context.Context, s stores.Reading, State State, span branches.Span, limit int) ([]Entry, error) {
	span2 := gotkv.TotalSpan()
	if span.Begin != "" {
		span2.Begin = []byte(span.Begin)
	}
	if span.End != "" {
		span2.End = []byte(span.End)
	}
	it := m.gotkv.NewIterator(s, State.Branches, span2)
	var ents []Entry
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

type VolumeEntry = gotnsop.VolumeEntry

func (m *Machine) PutVolumeEntry(ctx context.Context, s stores.RW, state State, entry VolumeEntry) (*State, error) {
	next, err := m.gotkv.Put(ctx, s, state.Volumes, entry.Key(nil), entry.Value(nil))
	if err != nil {
		return nil, err
	}
	state.Volumes = *next
	return &state, nil
}

func (m *Machine) GetVolumeEntry(ctx context.Context, s stores.Reading, state State, volOID blobcache.OID) (*VolumeEntry, error) {
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

func (m *Machine) DeleteVolumeEntry(ctx context.Context, s stores.RW, state State, volOID blobcache.OID) (*State, error) {
	next, err := m.gotkv.Delete(ctx, s, state.Volumes, volOID[:])
	if err != nil {
		return nil, err
	}
	state.Volumes = *next
	return &state, nil
}
