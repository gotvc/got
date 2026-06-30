package porting

import (
	"context"
	"encoding/binary"
	"fmt"
	"io/fs"

	"github.com/gotvc/got/src/gotfs"
	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/tai64"
	"go.etcd.io/bbolt"
)

type FileInfo struct {
	Path       string
	ModifiedAt tai64.TAI64N
	Mode       fs.FileMode
	Size       int64
	ByGot      bool
}

type DB struct {
	db        *bbolt.DB
	paramHash [32]byte
}

func NewDB(db *bbolt.DB, paramHash [32]byte) *DB {
	return &DB{
		db:        db,
		paramHash: paramHash,
	}
}

var (
	bucketDirstate = []byte("dirstate")
	bucketFSRoots  = []byte("fsroots")
)

func (db *DB) ensureBuckets(tx *bbolt.Tx) error {
	for _, name := range [][]byte{bucketDirstate, bucketFSRoots} {
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) PutInfo(ctx context.Context, ent FileInfo) error {
	if ent.Path == "" {
		return fmt.Errorf("import/export DB does not allow the root to be stored")
	}
	return db.db.Update(func(tx *bbolt.Tx) error {
		if err := db.ensureBuckets(tx); err != nil {
			return err
		}
		b := tx.Bucket(bucketDirstate)
		if err := b.Put([]byte(ent.Path), marshalInfo(&ent)); err != nil {
			return err
		}
		fsroots := tx.Bucket(bucketFSRoots)
		key := fsrootKey(db.paramHash, ent.Path)
		return fsroots.Delete(key)
	})
}

func (db *DB) GetInfo(ctx context.Context, p string, dst *FileInfo) (bool, error) {
	var found bool
	err := db.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketDirstate)
		if b == nil {
			return nil
		}
		data := b.Get([]byte(p))
		if data == nil {
			return nil
		}
		dst.Path = p
		if err := unmarshalInfo(data, dst); err != nil {
			return err
		}
		found = true
		return nil
	})
	return found, err
}

func (db *DB) NewInfoIterator() *DBInfoIterator {
	return NewDBInfoIterator(db)
}

func (db *DB) Delete(ctx context.Context, p string) error {
	return db.db.Update(func(tx *bbolt.Tx) error {
		d := tx.Bucket(bucketDirstate)
		if d != nil {
			if err := d.Delete([]byte(p)); err != nil {
				return err
			}
		}
		f := tx.Bucket(bucketFSRoots)
		if f != nil {
			if err := f.Delete(fsrootKey(db.paramHash, p)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *DB) PutFSRoot(ctx context.Context, p string, modt tai64.TAI64N, fsroot gotfs.Root) error {
	var info FileInfo
	if ok, err := db.GetInfo(ctx, p, &info); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("cannot add file data before info has been added")
	}
	if info.ModifiedAt != modt {
		return fmt.Errorf("modtime does not match")
	}
	return db.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketFSRoots)
		if b == nil {
			return fmt.Errorf("fsroots bucket missing")
		}
		return b.Put(fsrootKey(db.paramHash, p), fsroot.Marshal(nil))
	})
}

func (db *DB) GetFSRoot(ctx context.Context, p string, dst *gotfs.Root) (bool, error) {
	var found bool
	err := db.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketFSRoots)
		if b == nil {
			return nil
		}
		data := b.Get(fsrootKey(db.paramHash, p))
		if data == nil {
			return nil
		}
		if err := dst.Unmarshal(data); err != nil {
			return err
		}
		found = true
		return nil
	})
	return found, err
}

func (db *DB) PutBoth(ctx context.Context, ent FileInfo, modt tai64.TAI64N, root gotfs.Root) error {
	if ent.Path == "" {
		return fmt.Errorf("import/export DB does not allow the root to be stored")
	}
	if ent.ModifiedAt != modt {
		return fmt.Errorf("modtime does not match")
	}
	return db.db.Update(func(tx *bbolt.Tx) error {
		if err := db.ensureBuckets(tx); err != nil {
			return err
		}
		d := tx.Bucket(bucketDirstate)
		if err := d.Put([]byte(ent.Path), marshalInfo(&ent)); err != nil {
			return err
		}
		f := tx.Bucket(bucketFSRoots)
		key := fsrootKey(db.paramHash, ent.Path)
		return f.Put(key, root.Marshal(nil))
	})
}

func marshalInfo(ent *FileInfo) []byte {
	buf := make([]byte, 25)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(ent.Mode))
	binary.LittleEndian.PutUint64(buf[4:12], uint64(ent.Size))
	if ent.ByGot {
		buf[12] = 1
	}
	copy(buf[13:25], ent.ModifiedAt.Marshal())
	return buf
}

func unmarshalInfo(data []byte, ent *FileInfo) error {
	if len(data) != 25 {
		return fmt.Errorf("invalid info data length: %d", len(data))
	}
	ent.Mode = fs.FileMode(binary.LittleEndian.Uint32(data[0:4]))
	ent.Size = int64(binary.LittleEndian.Uint64(data[4:12]))
	ent.ByGot = data[12] != 0
	return ent.ModifiedAt.UnmarshalBinary(data[13:25])
}

func fsrootKey(ph [32]byte, p string) []byte {
	buf := make([]byte, 32+len(p))
	copy(buf, ph[:])
	copy(buf[32:], p)
	return buf
}

type DBInfoIterator = streams.SeqErr[FileInfo]

func NewDBInfoIterator(db *DB) *DBInfoIterator {
	seq := func(yield func(FileInfo, error) bool) {
		err := db.db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket(bucketDirstate)
			if b == nil {
				return nil
			}
			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				var ent FileInfo
				ent.Path = string(k)
				if err := unmarshalInfo(v, &ent); err != nil {
					if !yield(FileInfo{}, err) {
						return nil
					}
					continue
				}
				if !yield(ent, nil) {
					return nil
				}
			}
			return nil
		})
		if err != nil {
			yield(FileInfo{}, err)
		}
	}
	return streams.NewSeqErr(seq)
}
