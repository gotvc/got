package cells

import (
	"bytes"
	"context"

	"github.com/brendoncarroll/go-state/cells"
	bolt "go.etcd.io/bbolt"
)

type boltCell struct {
	db         *bolt.DB
	bucketPath []string
	key        []byte
}

func NewBoltCell(db *bolt.DB, bucketPath []string, key []byte) Cell {
	if len(bucketPath) < 1 {
		panic("len(path) must be >= 1")
	}
	return &boltCell{
		db:         db,
		bucketPath: bucketPath,
		key:        key,
	}
}

func (c *boltCell) CAS(ctx context.Context, actual, prev, next []byte) (bool, int, error) {
	if len(next) > c.MaxSize() {
		return false, 0, cells.ErrTooLarge{}
	}
	path := c.bucketPath
	var swapped bool
	var n int
	// have to be careful to always assign to swapped and actual every time this function is called, unless it errors
	err := c.db.Batch(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(path[0]))
		if err != nil {
			return err
		}
		path = path[1:]
		for len(path) > 0 {
			b, err = tx.CreateBucketIfNotExists([]byte(path[0]))
			if err != nil {
				return err
			}
			path = path[1:]
		}
		key := c.key
		v := b.Get([]byte(key))
		if !bytes.Equal(v, prev) {
			swapped = false
			n = copy(actual, v)
		} else {
			if err := b.Put(key, next); err != nil {
				return err
			}
			swapped = true
			n = copy(actual, next)
		}
		return nil
	})
	if err != nil {
		return false, 0, err
	}
	return swapped, n, nil
}

func (c *boltCell) Read(ctx context.Context, buf []byte) (int, error) {
	var n int
	if err := c.db.View(func(tx *bolt.Tx) error {
		path := c.bucketPath
		b := tx.Bucket([]byte(path[0]))
		if b == nil {
			return nil
		}
		path = path[1:]
		for len(path) > 0 {
			b = b.Bucket([]byte(path[0]))
			if b == nil {
				return nil
			}
			path = path[1:]
		}
		key := c.key
		v := b.Get(key)
		n = copy(buf, v)
		return nil
	}); err != nil {
		return 0, err
	}
	return n, nil
}

func (c *boltCell) MaxSize() int {
	return 1 << 16
}
