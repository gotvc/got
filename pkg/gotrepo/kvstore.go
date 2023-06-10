package gotrepo

import (
	"bytes"
	"context"

	"errors"

	"github.com/brendoncarroll/go-state"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/exp/slices"
)

type boltKVStore struct {
	db     *bolt.DB
	bucket string
}

func newBoltKVStore(db *bolt.DB, bucket string) state.KVStore[[]byte, []byte] {
	return boltKVStore{
		db:     db,
		bucket: bucket,
	}
}

func (s boltKVStore) Put(ctx context.Context, k, v []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(s.bucket))
		if err != nil {
			return err
		}
		return b.Put(k, v)
	})
}

func (s boltKVStore) Get(ctx context.Context, k []byte) ([]byte, error) {
	var ret []byte
	if err := s.db.View(func(tx *bolt.Tx) error {
		ret = nil
		b := tx.Bucket([]byte(s.bucket))
		if b == nil {
			return state.ErrNotFound
		}
		v := b.Get(k)
		if v == nil {
			return state.ErrNotFound
		}
		ret = append([]byte{}, v...)
		return nil
	}); err != nil {
		return nil, err
	}
	return ret, nil
}

func (s boltKVStore) List(ctx context.Context, span state.Span[[]byte], ks [][]byte) (int, error) {
	if len(ks) == 0 {
		return 0, errors.New("List called with empty buffer")
	}
	var n int
	err := s.db.View(func(tx *bolt.Tx) error {
		n = 0
		b := tx.Bucket([]byte(s.bucket))
		if b == nil {
			return nil
		}
		c := b.Cursor()
		var key []byte
		if lower, ok := span.LowerBound(); ok {
			key, _ = c.Seek(lower)
		} else {
			key, _ = c.First()
		}
		for n < len(ks) && key != nil && span.Compare(key, bytes.Compare) >= 0 {
			if span.Contains(key, bytes.Compare) {
				ks[n] = slices.Clone(key)
				n++
			}
			key, _ = c.Next()
		}
		return nil
	})
	return n, err
}

func (s boltKVStore) Delete(ctx context.Context, k []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.bucket))
		if b == nil {
			return nil
		}
		return b.Delete(k)
	})
}
