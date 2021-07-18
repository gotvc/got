package gotrepo

import (
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

func (cm *cellManager) Get(id CellID) (Cell, error) {
	key := cm.keyFromID(id)
	return cells.NewBoltCell(cm.db, cm.bucketPath, key), nil
}

func (cm *cellManager) Delete(ctx context.Context, id CellID) error {
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
