package cadata

import (
	"context"
	"os"

	"github.com/blobcache/blobcache/pkg/blobs"
	bolt "go.etcd.io/bbolt"
)

type boltStore struct {
	db     *bolt.DB
	bucket string
}

func NewBoltStore(db *bolt.DB, bucket string) Store {
	return &boltStore{
		db:     db,
		bucket: bucket,
	}
}

func (s *boltStore) Post(ctx context.Context, data []byte) (ID, error) {
	id := blobs.Hash(data)
	err := s.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(s.bucket))
		if err != nil {
			return err
		}
		v := b.Get(id[:])
		if len(v) > 0 {
			return nil
		}
		return b.Put(id[:], data)
	})
	if err != nil {
		return ID{}, err
	}
	return id, nil
}

func (s *boltStore) GetF(ctx context.Context, id blobs.ID, fn func([]byte) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.bucket))
		if b == nil {
			return nil
		}
		v := b.Get(id[:])
		if v == nil {
			return os.ErrNotExist
		}
		return fn(v)
	})
}

func (s *boltStore) Delete(ctx context.Context, id blobs.ID) error {
	panic("not implemented")
}

func (s *boltStore) Exists(ctx context.Context, id blobs.ID) (bool, error) {
	panic("not implemented")
}

func (s *boltStore) List(ctx context.Context, prefix []byte, ids []ID) (int, error) {
	panic("not implemented")
}
