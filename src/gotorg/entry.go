package gotorg

import (
	"context"

	"go.brendoncarroll.net/exp/streams"

	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/gotorg/internal/gotorgop"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/marks"
)

type (
	VolumeAlias = gotorgop.VolumeAlias
	VolumeEntry = gotorgop.VolumeEntry
)

// GetAlias looks up an entry in the branches table.
func (m *Machine) GetAlias(ctx context.Context, s stores.Reading, state State, name string) (*gotorgop.VolumeAlias, error) {
	val, err := m.gotkv.Get(ctx, s, state.VolumeNames, []byte(name))
	if err != nil {
		if gotkv.IsErrKeyNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	entry, err := gotorgop.ParseVolumeAlias([]byte(name), val)
	if err != nil {
		return nil, err
	}
	return &entry, nil
}

// PutAlias inserts or overwrites an entry in the branches table.
func (m *Machine) PutAlias(ctx context.Context, s stores.RW, state State, entry gotorgop.VolumeAlias, secret *gotorgop.Secret) (*State, error) {
	mut1 := putAlias(entry)
	aliasState, err := m.gotkv.Edit(ctx, s, state.VolumeNames, mut1)
	if err != nil {
		return nil, err
	}
	state.VolumeNames = *aliasState
	nextState, err := m.FulfillObligations(ctx, s, state, entry.Name, secret)
	if err != nil {
		return nil, err
	}
	return nextState, nil
}

// DeleteAlias deletes an alias from the namespace.
func (m *Machine) DeleteAlias(ctx context.Context, s stores.RW, State State, name string) (*State, error) {
	entsState, err := m.gotkv.Delete(ctx, s, State.VolumeNames, []byte(name))
	if err != nil {
		return nil, err
	}
	State.VolumeNames = *entsState
	return &State, nil
}

func putAlias(entry gotorgop.VolumeAlias) gotkv.Edit {
	k := entry.Key(nil)
	return gotkv.Edit{
		Span: gotkv.SingleKeySpan(k),
		Entries: []gotkv.Entry{
			{
				Key:   entry.Key(nil),
				Value: entry.Value(nil),
			},
		},
	}
}

func (m *Machine) ListBranches(ctx context.Context, s stores.Reading, state State, span marks.Span, limit int) ([]gotorgop.VolumeAlias, error) {
	span2 := gotkv.TotalSpan()
	if span.Begin != "" {
		span2.Begin = []byte(span.Begin)
	}
	if span.End != "" {
		span2.End = []byte(span.End)
	}
	it := m.gotkv.NewIterator(s, state.VolumeNames, span2)
	var ents []gotorgop.VolumeAlias
	for {
		ent, err := streams.Next(ctx, it)
		if err != nil {
			if streams.IsEOS(err) {
				break
			}
			return nil, err
		}

		entry, err := gotorgop.ParseVolumeAlias(ent.Key, ent.Value)
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
