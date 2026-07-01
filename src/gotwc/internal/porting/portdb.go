package porting

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"path"
	"sync/atomic"

	"github.com/gotvc/got/src/gotfs"
	"go.brendoncarroll.net/exp/sbe"
	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/tai64"
	"go.etcd.io/bbolt"
)

// InfoEntry is a how file info is stored in the database.
type InfoEntry struct {
	// Path is the key for an InfoEntry
	Path string

	// Info is the value for an InfoEntry
	Info FileInfo
}

func parseInfoEntry(k, v []byte) (ret InfoEntry, _ error) {
	if err := ret.Info.Unmarshal(v); err != nil {
		return ret, err
	}
	ret.Path = string(k)
	return ret, nil
}

func (ient InfoEntry) Key(out []byte) []byte {
	return append(out, ient.Path...)
}

func (ient InfoEntry) Value(out []byte) []byte {
	// TODO: use sbe package to serialize.
	data, _ := json.Marshal(ient.Info)
	return append(out, data...)
}

// ExtentKey is a key in the extents table
type ExtentKey struct {
	Path      string
	ParamHash [32]byte
	EndAt     uint64
}

func (k ExtentKey) Marshal(out []byte) []byte {
	out = append(out, []byte(k.Path)...)
	out = append(out, k.ParamHash[:]...)
	out = sbe.AppendUint64(out, k.EndAt)
	return out
}

func (k *ExtentKey) Unmarshal(out []byte) error {
	return nil
}

// ExtentValue is the value stored in the extents table
type ExtentValue struct {
	Extent     gotfs.Extent
	ModifiedAt tai64.TAI64N
}

type ExtentEntry struct {
	EndAt  uint64
	Extent gotfs.Extent
}

type DB struct {
	db        *bbolt.DB
	doneSetup atomic.Bool
}

func NewDB(db *bbolt.DB) *DB {
	return &DB{db: db}
}

const (
	bucketInfos   = "infos"
	bucketExtents = "extents"
)

func (db *DB) ensureBuckets(tx *bbolt.Tx) error {
	if done := db.doneSetup.Load(); done {
		return nil
	}
	for _, name := range []string{bucketInfos, bucketExtents} {
		if _, err := tx.CreateBucketIfNotExists([]byte(name)); err != nil {
			return err
		}
	}
	db.doneSetup.Store(true)
	return nil
}

// UpdateInfo updates the cached file info for a path.
// If the file has changed in anyway, then all of the extents are invalidated.
// It returns true if the path has changed, and will require reimport.
func (db *DB) UpdateInfo(ctx context.Context, p string, info FileInfo) (bool, error) {
	p = CleanPath(p)
	var hasChanged bool
	err := db.db.Update(func(tx *bbolt.Tx) error {
		if err := db.ensureBuckets(tx); err != nil {
			return err
		}
		b := tx.Bucket([]byte(bucketInfos))
		k := []byte(p)

		if val := b.Get(k); val != nil {
			var oldInfo FileInfo
			if err := oldInfo.Unmarshal(val); err != nil {
				return err
			}
			if HasChanged(&oldInfo, &info) {
				hasChanged = true
				if err := invalidateExtents(tx, p); err != nil {
					return err
				}
			} else {
				return nil // nothing to do.
			}
		} else {
			// no previous entry, need update
			hasChanged = true
		}
		return b.Put(k, info.Marshal(nil))
	})
	return hasChanged, err
}

func (db *DB) putInfoEntry(ctx context.Context, ient InfoEntry) error {
	_, err := db.UpdateInfo(ctx, ient.Path, ient.Info)
	return err
}

// GetInfo returns the last known info about the file.
func (db *DB) GetInfo(ctx context.Context, p string, dst *FileInfo) (bool, error) {
	var found bool
	err := db.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketInfos))
		if b == nil {
			return nil
		}
		val := b.Get([]byte(p))
		found = true
		return dst.Unmarshal(val)
	})
	return found, err
}

func (db *DB) Delete(ctx context.Context, p string) error {
	return db.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketInfos))
		if b != nil {
			if err := b.Delete([]byte(p)); err != nil {
				return err
			}
		}
		return invalidateExtents(tx, p)
	})
}

func (db *DB) AddExtents(ctx context.Context, p string, paramHash [32]byte, ents []gotfs.Entry) error {
	return db.db.Update(func(tx *bbolt.Tx) error {
		if err := db.ensureBuckets(tx); err != nil {
			return err
		}
		b := tx.Bucket([]byte(bucketExtents))
		for _, ent := range ents {
			if ent.IsInfo() {
				continue
			}
			if err := putExtent(b, p, paramHash, ent.EndAt(), ent.Extent); err != nil {
				return err
			}
		}
		return nil
	})
}

// GetExtents gets extents for (p, paramHash) and appends them to out
func (db *DB) GetExtents(ctx context.Context, p string, paramHash [32]byte, out []gotfs.Entry) ([]gotfs.Entry, error) {
	err := db.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketExtents))
		if b == nil {
			return fmt.Errorf("no extents for path + paramHash")
		}
		prefix := extentPrefix(p, paramHash)
		c := b.Cursor()
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			ee, err := parseExtentEntry(k, b.Get(k))
			if err != nil {
				return err
			}
			out = append(out, gotfs.Entry{
				Key:   gotfs.NewExtentKey(p, ee.EndAt),
				Value: gotfs.Value{Extent: ee.Extent},
			})
		}
		return nil
	})
	return out, err
}

func putExtent(b *bbolt.Bucket, p string, paramHash [32]byte, endAt uint64, ext gotfs.Extent) error {
	k := extentKey(p, paramHash, endAt)
	val, err := ext.MarshalBinary()
	if err != nil {
		return err
	}
	return b.Put(k, val)
}

func parseExtentEntry(k, v []byte) (ExtentEntry, error) {
	endAt := binary.BigEndian.Uint64(k[len(k)-8:])
	var ext gotfs.Extent
	if err := ext.UnmarshalBinary(v); err != nil {
		return ExtentEntry{}, err
	}
	return ExtentEntry{
		EndAt:  endAt,
		Extent: ext,
	}, nil
}

// NewInfoIterator returns an iterator over all tracked paths.
func (db *DB) NewInfoIterator() *DBInfoIterator {
	return newDBInfoIterator(db)
}

func deleteInfo(tx *bbolt.Tx, p string) error {
	b := tx.Bucket([]byte(bucketInfos))
	if b == nil {
		return nil
	}
	return b.Delete([]byte(p))
}

// invalidateExtents deletes all cached extents across all paramHashes.
func invalidateExtents(tx *bbolt.Tx, p string) error {
	b := tx.Bucket([]byte(bucketExtents))
	if b == nil {
		return nil // nothing to do
	}
	prefix := append([]byte(p), 0)
	c := b.Cursor()
	for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		if err := b.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

func extentKey(p string, paramHash [32]byte, endAt uint64) []byte {
	buf := append([]byte(p), 0)
	buf = append(buf, paramHash[:]...)
	buf = binary.BigEndian.AppendUint64(buf, endAt)
	return buf
}

func extentPrefix(p string, paramHash [32]byte) []byte {
	buf := append([]byte(p), 0)
	buf = append(buf, paramHash[:]...)
	return buf
}

type DBInfoIterator = streams.SeqErr[InfoEntry]

func newDBInfoIterator(db *DB) *DBInfoIterator {
	seq := func(yield func(InfoEntry, error) bool) {
		err := db.db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(bucketInfos))
			if b == nil {
				return nil
			}
			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				var ent InfoEntry
				ent, err := parseInfoEntry(k, v)
				if err != nil {
					return err
				}
				if !yield(ent, err) {
					return nil
				}
			}
			return nil
		})
		if err != nil {
			yield(InfoEntry{}, err)
		}
	}
	return streams.NewSeqErr(seq)
}

func CleanPath(p string) string {
	p = path.Clean(p)
	switch p {
	case ".", "/":
		p = ""
	}
	return p
}
