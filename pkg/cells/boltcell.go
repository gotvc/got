package cells

import (
	"bytes"
	"context"

	bolt "go.etcd.io/bbolt"
)

type boltCell struct {
	db   *bolt.DB
	path []string
}

func NewBoltCell(db *bolt.DB, path []string) Cell {
	if len(path) < 2 {
		panic("len(path) must be > 2")
	}
	return &boltCell{db: db, path: path}
}

func (c *boltCell) CAS(ctx context.Context, prev, next []byte) (bool, []byte, error) {
	path := c.path
	var swapped bool
	var actual []byte
	// have to be careful to always assign to swapped and actual every time this function is called, unless it errors
	err := c.db.Batch(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(path[0]))
		if err != nil {
			return err
		}
		path = path[1:]
		for len(path) > 1 {
			b, err = tx.CreateBucketIfNotExists([]byte(path[0]))
			if err != nil {
				return err
			}
			path = path[1:]
		}
		key := []byte(path[0])
		v := b.Get([]byte(key))
		if !bytes.Equal(v, prev) {
			swapped = false
			actual = append([]byte{}, v...)
			return nil
		}
		if err := b.Put(key, next); err != nil {
			return err
		}
		actual = next
		swapped = true
		return nil
	})
	if err != nil {
		return false, nil, err
	}
	return swapped, actual, nil
}

func (c *boltCell) Get(ctx context.Context) ([]byte, error) {
	var data []byte
	if err := c.db.View(func(tx *bolt.Tx) error {
		path := c.path
		b := tx.Bucket([]byte(path[0]))
		if b == nil {
			return nil
		}
		path = path[1:]
		for len(path) > 1 {
			b = b.Bucket([]byte(path[0]))
			if b == nil {
				return nil
			}
			path = path[1:]
		}
		key := []byte(path[0])
		v := b.Get(key)
		data = append([]byte{}, v...)
		return nil
	}); err != nil {
		return nil, err
	}
	return data, nil
}
