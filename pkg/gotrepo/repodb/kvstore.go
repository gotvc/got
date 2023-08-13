package repodb

import (
	"bytes"
	"context"
	"errors"

	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/kv"
	"github.com/dgraph-io/badger/v3"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
)

type badgerKVStore struct {
	db  *badger.DB
	tid TableID
}

func NewKVStore(db *badger.DB, tid TableID) kv.Store[[]byte, []byte] {
	return badgerKVStore{
		db:  db,
		tid: tid,
	}
}

func (s badgerKVStore) Put(ctx context.Context, k, v []byte) error {
	return s.db.Update(func(tx *badger.Txn) error {
		k := Key(nil, s.tid, k)
		return tx.Set(k, v)
	})
}

func (s badgerKVStore) Get(ctx context.Context, k []byte, dst *[]byte) error {
	return s.db.View(func(tx *badger.Txn) error {
		item, err := tx.Get(Key(nil, s.tid, k))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return state.ErrNotFound
			}
			return err
		}

		*dst, err = item.ValueCopy(*dst)
		return err
	})
}

func (s badgerKVStore) Exists(ctx context.Context, k []byte) (bool, error) {
	return kv.ExistsUsingList(ctx, s, k)
}

func (s badgerKVStore) List(ctx context.Context, span state.Span[[]byte], ks [][]byte) (int, error) {
	if len(ks) == 0 {
		return 0, errors.New("List called with empty buffer")
	}
	var n int
	err := s.db.View(func(tx *badger.Txn) error {
		n = 0
		opts := badger.DefaultIteratorOptions
		opts.Prefix = s.tid.Bytes()
		iter := tx.NewIterator(opts)
		defer iter.Close()

		if lb, ok := span.LowerBound(); ok {
			k := Key(nil, s.tid, lb)
			if !span.IncludesLower() {
				k = kvstreams.KeyAfter(k)
			}
			iter.Seek(k)
		} else {
			iter.Seek(nil)
		}
		for ; iter.Valid() && n < len(ks); iter.Next() {
			item := iter.Item()
			if span.Contains(item.Key()[4:], bytes.Compare) {
				if ks[n] == nil {
					ks[n] = []byte{}
				}
				ks[n] = append(ks[n][:0], item.Key()[4:]...)
				n++
			} else {
				break
			}
		}
		return nil
	})
	return n, err
}

func (s badgerKVStore) Delete(ctx context.Context, k []byte) error {
	return s.db.Update(func(tx *badger.Txn) error {
		return tx.Delete(Key(nil, s.tid, k))
	})
}
