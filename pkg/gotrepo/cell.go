package gotrepo

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/gotvc/got/pkg/cells"
	bolt "go.etcd.io/bbolt"
)

type CellID uint64

type cellManager struct {
	db         *bolt.DB
	bucketPath []string
}

func newCellManager(db *bolt.DB, bucketPath []string) *cellManager {
	return &cellManager{
		db:         db,
		bucketPath: bucketPath,
	}
}

func (cm *cellManager) Open(id CellID) Cell {
	key := cm.keyFromID(id)
	return newBoltCell(cm.db, cm.bucketPath, key)
}

func (cm *cellManager) Drop(ctx context.Context, id CellID) error {
	key := cm.keyFromID(id)
	return cm.db.Update(func(tx *bolt.Tx) error {
		b, err := bucketFromTx(tx, cm.bucketPath)
		if err != nil {
			return err
		}
		return b.Delete(key)
	})
}

func (cm *cellManager) keyFromID(id CellID) []byte {
	key := [8]byte{}
	binary.BigEndian.PutUint64(key[:], uint64(id))
	return key[:]
}

type boltCell struct {
	db         *bolt.DB
	bucketPath []string
	key        []byte
}

func newBoltCell(db *bolt.DB, bucketPath []string, key []byte) Cell {
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
