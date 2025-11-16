package gotns

import (
	"context"

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
