package gotrepo

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"

	"github.com/dgraph-io/badger/v3"
	"github.com/gotvc/got/pkg/cells"
	"github.com/gotvc/got/pkg/gotrepo/repodb"
	"github.com/pkg/errors"
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
}

func newBadgerCell(db *badger.DB, key []byte) Cell {
	return &badgerCell{
		db:  db,
		key: key,
	}
}

func (c *badgerCell) CAS(ctx context.Context, actual, prev, next []byte) (bool, int, error) {
	if len(next) > c.MaxSize() {
		return false, 0, cells.ErrTooLarge{}
	}
	var swapped bool
	var n int
	if err := c.db.Update(func(tx *badger.Txn) error {
		var current []byte
		item, err := tx.Get(c.key)
		if errors.Is(err, badger.ErrKeyNotFound) {
		} else if err != nil {
			return err
		} else {
			var err error
			current, err = item.ValueCopy(current)
			if err != nil {
				return err
			}
		}
		if bytes.Equal(prev, current) {
			if err := tx.Set(c.key, next); err != nil {
				return err
			}
			swapped = true
			n = copy(actual, next)
		} else {
			swapped = false
			n = copy(actual, current)
		}
		return nil
	}); err != nil {
		return false, 0, err
	}
	return swapped, n, nil
}

func (c *badgerCell) Read(ctx context.Context, buf []byte) (int, error) {
	var n int
	if err := c.db.View(func(tx *badger.Txn) error {
		key := c.key
		item, err := tx.Get(key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		} else if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			if len(buf) < len(v) {
				return io.ErrShortBuffer
			}
			n = copy(buf, v)
			return nil
		})
	}); err != nil {
		return 0, err
	}
	return n, nil
}

func (c *badgerCell) MaxSize() int {
	return MaxCellSize
}
