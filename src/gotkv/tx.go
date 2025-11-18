package gotkv

import (
	"context"
	"slices"

	"github.com/gotvc/got/src/internal/stores"
)

// Tx allows operations to be batched.
type Tx struct {
	m    *Machine
	s    stores.RW
	prev Root

	edits map[string][]byte
}

func (m *Machine) NewTx(s stores.RW, prev Root) *Tx {
	return &Tx{m: m, s: s, prev: prev}
}

func (tx *Tx) Finish(ctx context.Context) (*Root, error) {
	edits := tx.edits
	var muts []Mutation
	for k, v := range edits {
		var mut Mutation
		switch v {
		case nil:
			// Deletion
			mut = Mutation{
				Span: SingleKeySpan([]byte(k)),
			}
		default:
			// Put
			mut = Mutation{
				Span: SingleKeySpan([]byte(k)),
				Entries: []Entry{
					{Key: []byte(k), Value: v},
				},
			}
		}
		muts = append(muts, mut)
	}
	nextRoot, err := tx.m.Mutate(ctx, tx.s, tx.prev, muts...)
	if err != nil {
		return nil, err
	}
	clear(tx.edits)
	tx.prev = *nextRoot
	return nextRoot, nil
}

func (tx *Tx) Put(ctx context.Context, key, value []byte) error {
	if tx.edits == nil {
		tx.edits = make(map[string][]byte)
	}
	tx.edits[string(key)] = slices.Clone(value)
	return nil
}

func (tx *Tx) Get(ctx context.Context, key []byte, dst *[]byte) (bool, error) {
	if val, exists := tx.edits[string(key)]; exists {
		*dst = append((*dst)[:0], val...)
		return true, nil
	}
	if tx.prev.Ref.IsZero() {
		return false, nil
	}
	if err := tx.m.GetF(ctx, tx.s, tx.prev, key, func(val []byte) error {
		*dst = append((*dst)[:0], val...)
		return nil
	}); err != nil {
		if IsErrKeyNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (tx *Tx) Delete(ctx context.Context, key []byte) error {
	if tx.edits == nil {
		tx.edits = make(map[string][]byte)
	}
	tx.edits[string(key)] = nil
	return nil
}
