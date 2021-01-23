package got

import (
	"bytes"
	"context"

	bolt "go.etcd.io/bbolt"
)

var _ Cell = &boltCell{}

const bucketCellData = "cell_data"

type boltCell struct {
	db *bolt.DB
	k  string
}

func newBoltCell(db *bolt.DB, k string) *boltCell {
	return &boltCell{
		db: db,
		k:  k,
	}
}

func (bc *boltCell) CAS(ctx context.Context, prev []byte, next []byte) (swapped bool, actual []byte, err error) {
	err = bc.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucketCellData))
		if err != nil {
			return err
		}
		v := b.Get([]byte(bc.k))
		if !bytes.Equal(v, prev) {
			swapped = false
			actual = append([]byte{}, v...)
			return nil
		}
		if err := b.Put([]byte(bc.k), next); err != nil {
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

func (bc *boltCell) Get(ctx context.Context) (data []byte, err error) {
	if err = bc.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketCellData))
		if b == nil {
			return nil
		}
		v := b.Get([]byte(bc.k))
		data = append([]byte{}, v...)
		return nil
	}); err != nil {
		return nil, err
	}
	return data, nil
}

func boltApply(db *bolt.DB, bucketName string, key []byte, fn func([]byte) ([]byte, error)) error {
	return db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}
		x := b.Get(key)
		y, err := fn(x)
		if err != nil {
			return err
		}
		return b.Put(key, y)
	})
}
