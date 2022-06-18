package gotrepo

import (
	"bytes"
	"context"

	"github.com/brendoncarroll/go-state"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
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
		b := tx.Bucket([]byte(s.bucket))
		if b == nil {
			return nil
		}
		ret = append([]byte{}, b.Get(k)...)
		return nil
	}); err != nil {
		return nil, err
	}
	if ret == nil {
		return nil, state.ErrNotFound
	}
	return ret, nil
}

func (s boltKVStore) List(ctx context.Context, span state.Span[[]byte], ks [][]byte) (int, error) {
	if len(ks) == 0 {
		return 0, errors.New("List called with empty buffer")
	}
	var n int
	appendKey := func(x []byte) {
		ks[n] = append(ks[n][:0], x...)
		n++
	}
	err := s.db.View(func(tx *bolt.Tx) error {
		n = 0
		b := tx.Bucket([]byte(s.bucket))
		if b == nil {
			return nil
		}
		c := b.Cursor()
		if lower, ok := span.LowerBound(); ok {
			key, _ := c.Seek(lower)
			if span.IncludesLower() || !bytes.Equal(key, lower) {
				if key != nil {
					appendKey(key)
				}
			}
		} else {
			key, _ := c.First()
			if key != nil {
				appendKey(key)
			}
		}
		for key, _ := c.Next(); key != nil; key, _ = c.Next() {
			if n >= len(ks) {
				break
			}
			if !span.Contains(key, bytes.Compare) {
				break
			}
			appendKey(key)
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
