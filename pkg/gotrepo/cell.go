package gotrepo

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"

	"github.com/dgraph-io/badger/v3"
	"go.brendoncarroll.net/state/cells"

	"github.com/gotvc/got/pkg/gotrepo/repodb"
)

const (
	cellIDSeq = repodb.TableID(0)
)

type CellID = uint64

type cellManager struct {
	db *badger.DB
}

func newCellManager(db *badger.DB) *cellManager {
	return &cellManager{
		db: db,
	}
}

func (cm *cellManager) Create(ctx context.Context) (ret CellID, _ error) {
	seq, err := cm.db.GetSequence(cellIDSeq.Bytes(), 1)
	if err != nil {
		return 0, err
	}
	cellID, err := seq.Next()
	if err != nil {
		return 0, err
	}
	if cellID == 0 {
		cellID, err = seq.Next()
		if err != nil {
			return 0, err
		}
	}
	return cellID, nil
}

func (cm *cellManager) Open(id CellID) Cell {
	key := cm.keyFromID(id)
	return newBadgerCell(cm.db, key)
}

func (cm *cellManager) Drop(ctx context.Context, id CellID) error {
	key := cm.keyFromID(id)
	return cm.db.Update(func(tx *badger.Txn) error {
		return tx.Delete(key)
	})
}

func (cm *cellManager) keyFromID(id CellID) []byte {
	key := [8]byte{}
	binary.BigEndian.PutUint64(key[:], uint64(id))
	return key[:]
}

type badgerCell struct {
	db  *badger.DB
	key []byte
	cells.BytesCellBase
}

func newBadgerCell(db *badger.DB, key []byte) cells.BytesCell {
	return &badgerCell{
		db:  db,
		key: key,
	}
}

func (c *badgerCell) CAS(ctx context.Context, actual *[]byte, prev, next []byte) (bool, error) {
	if len(next) > c.MaxSize() {
		return false, cells.ErrTooLarge{}
	}
	var swapped bool
	if err := c.db.Update(func(tx *badger.Txn) error {
		var current []byte
		item, err := tx.Get(c.key)
		if err == nil {
			var err error
			current, err = item.ValueCopy(current)
			if err != nil {
				return err
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		if bytes.Equal(current, prev) {
			if err := tx.Set(c.key, next); err != nil {
				return err
			}
			swapped = true
			cells.CopyBytes(actual, next)
		} else {
			swapped = false
			cells.CopyBytes(actual, current)
		}
		return nil
	}); err != nil {
		return false, err
	}
	return swapped, nil
}

func (c *badgerCell) Load(ctx context.Context, dst *[]byte) error {
	if err := c.db.View(func(tx *badger.Txn) error {
		key := c.key
		item, err := tx.Get(key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			cells.CopyBytes(dst, nil)
			return nil
		} else if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			cells.CopyBytes(dst, v)
			return nil
		})
	}); err != nil {
		return err
	}
	return nil
}

func (c *badgerCell) MaxSize() int {
	return MaxCellSize
}
