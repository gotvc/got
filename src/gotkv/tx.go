package gotkv

import (
	"bytes"
	"context"
	"slices"

	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/streams"
)

// Tx allows operations to be batched.
type Tx struct {
	m    *Machine
	s    stores.RW
	prev Root

	edits []Edit
}

func (m *Machine) NewTxEmpty(s stores.RW) *Tx {
	return m.NewTx(s, Root{})
}

func (m *Machine) NewTx(s stores.RW, prev Root) *Tx {
	return &Tx{
		m:    m,
		s:    s,
		prev: prev,
	}
}

func (tx *Tx) Queued() int {
	return len(tx.edits)
}

// Flush writes all in queued mutations to the key-value store.
func (tx *Tx) Flush(ctx context.Context) (*Root, error) {
	if tx.prev.Ref.IsZero() {
		r, err := tx.m.NewEmpty(ctx, tx.s)
		if err != nil {
			return nil, err
		}
		tx.prev = *r
	}
	nextRoot, err := tx.m.Edit(ctx, tx.s, tx.prev, tx.edits...)
	if err != nil {
		return nil, err
	}
	tx.edits = tx.edits[:0]
	tx.prev = *nextRoot
	return nextRoot, nil
}

func (tx *Tx) Put(ctx context.Context, key, value []byte) error {
	key, value = slices.Clone(key), slices.Clone(value)
	tx.edits = append(tx.edits, Edit{
		Span: SingleKeySpan(key),
		Entries: []Entry{
			{Key: key, Value: value},
		},
	})
	return nil
}

func (tx *Tx) Get(ctx context.Context, key []byte, dst *[]byte) (bool, error) {
	if i, yes := editsContain(tx.edits, key); yes {
		// we have edit this key in this transaction, need to return in-mem version
		// which may have been deleted.
		e := tx.edits[i]
		return getFromSlice(e.Entries, key, dst)
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

// getFromSlice retrieves an entry by key from ents.
// ents is assumed to be authoritive for some keyspace containing `key`.
func getFromSlice(ents []Entry, key []byte, val *[]byte) (bool, error) {
	for _, ent := range ents {
		if bytes.Equal(ent.Key, key) {
			*val = append((*val)[:0], ent.Value...)
			return true, nil
		}
	}
	return false, nil
}

func (tx *Tx) Delete(ctx context.Context, key []byte) error {
	tx.edits = append(tx.edits, Edit{
		Span:    SingleKeySpan(key),
		Entries: nil,
	})
	return nil
}

func (tx *Tx) IterateFlushed(ctx context.Context, span Span) *Iterator {
	return tx.m.NewIterator(tx.s, tx.prev, span)
}

// Iterate returns an iterator for the current state of the transaction.
// - Flush must be called before Iterate, or it panics.
func (tx *Tx) Iterate(ctx context.Context, span Span) *TxIterator {
	if len(tx.edits) > 0 {
		// TODO: lift this restriction
		panic("Iterate cannot be called with pending edits")
	}
	base := tx.IterateFlushed(ctx, span)
	oj := streams.NewOJoiner(
		streams.NewPeeker(streams.NewMutator(base, func(ent *Entry) bool {
			if editsDelete(tx.edits, ent.Key) {
				return false
			}
			if val, yes := editsPut(tx.edits, ent.Key); yes {
				ent.Value = append(ent.Value[:0], val...)
			}
			return true
		}), copyEntry),
		&localTxIterator{tx: tx, span: span},
		compareEntries,
	)
	return &TxIterator{oj: oj}
}

type TxIterator struct {
	oj *streams.OJoiner[Entry, Entry]
}

func (it *TxIterator) Next(ctx context.Context, dst []Entry) (int, error) {
	var n int
	var x streams.OJoined[Entry, Entry]
	for i := range dst {
		if err := streams.NextUnit(ctx, it.oj, &x); err != nil {
			if streams.IsEOS(err) {
				if i == 0 {
					return 0, streams.EOS()
				} else {
					break
				}
			}
			return 0, err
		}
		// right takes precedent over left
		if x.Right.Ok {
			copyEntry(&dst[i], x.Right.X)
		} else if x.Left.Ok {
			copyEntry(&dst[i], x.Left.X)
		} else {
			panic("unreachable")
		}
		n++
	}
	return n, nil
}

type localTxIterator struct {
	tx   *Tx
	span Span
}

func (lti *localTxIterator) Next(ctx context.Context, dst []Entry) (int, error) {
	// TODO: this will allow us to iterate over unflushed Edits
	return 0, streams.EOS()
}

func (lti *localTxIterator) Peek(ctx context.Context, dst *Entry) error {
	// TODO: this will allow us to iterate over unflushed Edits
	return streams.EOS()
}

// editsContain returns the largest (most recent) Edit
// which affects the key.
func editsContain(edits []Edit, key []byte) (int, bool) {
	for i := len(edits) - 1; i >= 0; i-- {
		if edits[i].Span.Contains(key) {
			return i, true
		}
	}
	return -1, false
}

// editsDelete returns true if the key is deleted by the edits
func editsDelete(edits []Edit, key []byte) bool {
	// work backwards from most recent to least recent
	for i := len(edits) - 1; i >= 0; i-- {
		if edits[i].Span.Contains(key) {
			return !slices.ContainsFunc(edits[i].Entries, func(ent Entry) bool {
				return bytes.Equal(key, ent.Key)
			})
		}
	}
	return false
}

// editsPut returns true if the edits put to key
func editsPut(edits []Edit, key []byte) ([]byte, bool) {
	// work backwards from most recent to least recent
	for i := len(edits) - 1; i >= 0; i-- {
		if edits[i].Span.Contains(key) {
			j := slices.IndexFunc(edits[i].Entries, func(ent Entry) bool {
				return bytes.Equal(key, ent.Key)
			})
			if j >= 0 {
				return edits[i].Entries[j].Value, true
			} else {
				return nil, false
			}
		}
	}
	return nil, false
}
