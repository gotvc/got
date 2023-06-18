package repodb

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v3"
)

type TableID uint32

func (tid TableID) AppendTo(x []byte) []byte {
	return binary.BigEndian.AppendUint32(x, uint32(tid))
}

func (tid TableID) Bytes() []byte {
	return tid.AppendTo(nil)
}

func (tid TableID) ByteArray() (ret [4]byte) {
	binary.BigEndian.PutUint32(ret[:], uint32(tid))
	return ret
}

func LookupTable(db *badger.DB, name string) (ret TableID, _ error) {
	err := db.View(func(tx *badger.Txn) error {
		tid, err := lookupTableTx(tx, name)
		if err != nil {
			return err
		}
		ret = tid
		return nil
	})
	return ret, err
}

// lookupTableTx returns the table ID for name, or 0 if there is no such table
func lookupTableTx(tx *badger.Txn, name string) (TableID, error) {
	item, err := tx.Get(Key(nil, 0, []byte(name)))
	if err == nil {
		var ret TableID
		if err := item.Value(func(v []byte) error {
			if len(v) != 4 {
				return errors.New("invalid TableID")
			}
			ret = TableID(binary.BigEndian.Uint32(v))
			return nil
		}); err != nil {
			return 0, err
		}
		return ret, nil
	} else if !errors.Is(err, badger.ErrKeyNotFound) {
		return 0, err
	}
	return 0, nil
}

// GetOrCreateTable returns the table id for a table with name if it exists, or creates a new table if it does not.
func GetOrCreateTable(db *badger.DB, name string) (ret TableID, _ error) {
	tableIDSeq := [4]byte{}
	err := db.Update(func(tx *badger.Txn) error {
		tid, err := lookupTableTx(tx, name)
		if err != nil {
			return err
		}
		if tid != 0 {
			ret = TableID(tid)
			return nil
		}
		// Create table
		newID, err := IncrUint32Tx(tx, tableIDSeq[:], 1)
		if err != nil {
			return err
		}
		ret = TableID(newID)
		return tx.Set(Key(nil, 0, []byte(name)), binary.BigEndian.AppendUint32(nil, newID))
	})
	if err != nil {
		return 0, err
	}
	return ret, nil
}

// IncrUint32Tx increments a big endian uint32 stored at key, by delta, and returns the result.
func IncrUint32Tx(tx *badger.Txn, key []byte, delta int32) (uint32, error) {
	n, err := GetUint32Tx(tx, key)
	if err != nil {
		return 0, err
	}
	n = uint32(int32(n) + delta)
	if err := tx.Set(key, binary.BigEndian.AppendUint32(nil, n)); err != nil {
		return 0, err
	}
	return n, nil
}

func GetUint32Tx(tx *badger.Txn, key []byte) (uint32, error) {
	item, err := tx.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, nil
	} else if err != nil {
		return 0, err
	} else {
		var n uint32
		if err := item.Value(func(v []byte) error {
			if len(v) != 4 {
				return fmt.Errorf("key does not contain uint32: %q", v)
			}
			n = binary.BigEndian.Uint32(v)
			return nil
		}); err != nil {
			return 0, err
		}
		return n, nil
	}
}

func IDFromString(x string) TableID {
	if len(x) > 4 {
		panic(x)
	}
	var buf [4]byte
	copy(buf[:], x)
	return TableID(binary.BigEndian.Uint32(buf[:]))
}

func Key(out []byte, tid TableID, key []byte) []byte {
	out = tid.AppendTo(out)
	out = append(out, key...)
	return out
}
