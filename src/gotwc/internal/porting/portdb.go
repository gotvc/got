package porting

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"path"
	"sync"
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

type Cache struct {
	mu        sync.RWMutex
	tx        *bbolt.Tx
	doneSetup atomic.Bool
}

func requireNonEmptyPath(p string) error {
	if p == "" {
		return fmt.Errorf("path cannot be empty")
	}
	return nil
}

func NewCache(tx *bbolt.Tx) Cache {
	return Cache{tx: tx}
}

const (
	bucketInfos   = "infos"
	bucketExtents = "extents"
	bucketKnown   = "known"
)

var stagedExtentHash [32]byte

func stagedInfoKey(p string) []byte {
	return append([]byte{0}, []byte(p)...)
}

func isStagedInfoKey(k []byte) bool {
	return len(k) > 0 && k[0] == 0
}

func (c *Cache) ensureBuckets(tx *bbolt.Tx) error {
	if done := c.doneSetup.Load(); done {
		return nil
	}
	for _, name := range []string{bucketInfos, bucketExtents, bucketKnown} {
		if _, err := tx.CreateBucketIfNotExists([]byte(name)); err != nil {
			return err
		}
	}
	c.doneSetup.Store(true)
	return nil
}

// UpdateInfo updates the cached file info for a path.
// If the file has changed in anyway, then all of the extents are invalidated.
// It returns true if the path has changed, and will require reimport.
func (c *Cache) UpdateInfo(ctx context.Context, p string, info FileInfo) (bool, error) {
	p = CleanPath(p)
	if err := requireNonEmptyPath(p); err != nil {
		return false, err
	}
	var hasChanged bool
	if err := c.ensureBuckets(c.tx); err != nil {
		return false, err
	}
	b := c.tx.Bucket([]byte(bucketInfos))
	k := []byte(p)

	if val := b.Get(k); val != nil {
		var oldInfo FileInfo
		if err := oldInfo.Unmarshal(val); err != nil {
			return false, err
		}
		if HasChanged(&oldInfo, &info) {
			hasChanged = true
			if err := invalidateExtents(c.tx, p); err != nil {
				return false, err
			}
		} else {
			return false, nil // nothing to do.
		}
	} else {
		// no previous entry, need update
		hasChanged = true
	}
	return hasChanged, b.Put(k, info.Marshal(nil))
}

func (c *Cache) putInfoEntry(ctx context.Context, ient InfoEntry) error {
	_, err := c.UpdateInfo(ctx, ient.Path, ient.Info)
	return err
}

// GetInfo returns the last known info about the file.
func (c *Cache) GetInfo(ctx context.Context, p string, dst *FileInfo) (bool, error) {
	p = CleanPath(p)
	if err := requireNonEmptyPath(p); err != nil {
		return false, err
	}
	var found bool
	b := c.tx.Bucket([]byte(bucketInfos))
	if b == nil {
		return false, nil
	}
	val := b.Get([]byte(p))
	found = val != nil
	return found, dst.Unmarshal(val)
}

func (c *Cache) Delete(ctx context.Context, p string) error {
	p = CleanPath(p)
	if err := requireNonEmptyPath(p); err != nil {
		return err
	}
	b := c.tx.Bucket([]byte(bucketInfos))
	if b != nil {
		if err := b.Delete([]byte(p)); err != nil {
			return err
		}
	}
	if err := c.SetKnown(ctx, p, nil); err != nil {
		return err
	}
	return invalidateExtents(c.tx, p)
}

func (c *Cache) SetKnown(ctx context.Context, p string, info *FileInfo) error {
	p = CleanPath(p)
	if err := requireNonEmptyPath(p); err != nil {
		return err
	}
	if err := c.ensureBuckets(c.tx); err != nil {
		return err
	}
	b := c.tx.Bucket([]byte(bucketKnown))
	if info == nil {
		return b.Delete([]byte(p))
	}
	return b.Put([]byte(p), info.Marshal(nil))
}

func (c *Cache) IsKnown(ctx context.Context, p string, actual FileInfo) (bool, error) {
	p = CleanPath(p)
	if err := requireNonEmptyPath(p); err != nil {
		return false, err
	}
	b := c.tx.Bucket([]byte(bucketKnown))
	if b == nil {
		return false, nil
	}
	data := b.Get([]byte(p))
	if len(data) == 0 {
		return false, nil
	}
	var knownInfo FileInfo
	if err := knownInfo.Unmarshal(data); err != nil {
		return false, err
	}
	if knownInfo.ModifiedAt != actual.ModifiedAt {
		return false, nil
	}
	return true, nil
}

func (c *Cache) DeleteKnownPrefix(ctx context.Context, p string) error {
	p = CleanPath(p)
	if err := requireNonEmptyPath(p); err != nil {
		return err
	}
	b := c.tx.Bucket([]byte(bucketKnown))
	if b == nil {
		return nil
	}
	prefix := append([]byte(p), 0)
	cur := b.Cursor()
	for k, _ := cur.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = cur.Next() {
		if err := b.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cache) deleteStagedEntries(ctx context.Context, p string) error {
	p = CleanPath(p)
	if err := requireNonEmptyPath(p); err != nil {
		return err
	}
	bInfos := c.tx.Bucket([]byte(bucketInfos))
	if bInfos != nil {
		if err := bInfos.Delete(stagedInfoKey(p)); err != nil {
			return err
		}
	}
	bExt := c.tx.Bucket([]byte(bucketExtents))
	if bExt != nil {
		prefix := extentPrefix(p, stagedExtentHash)
		cur := bExt.Cursor()
		for k, _ := cur.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = cur.Next() {
			if err := bExt.Delete(k); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Cache) ReplaceStagedEntries(ctx context.Context, p string, ents []gotfs.Entry) error {
	p = CleanPath(p)
	if err := requireNonEmptyPath(p); err != nil {
		return err
	}
	if err := c.ensureBuckets(c.tx); err != nil {
		return err
	}
	if err := c.deleteStagedEntries(ctx, p); err != nil {
		return err
	}
	bInfos := c.tx.Bucket([]byte(bucketInfos))
	bExt := c.tx.Bucket([]byte(bucketExtents))
	for _, ent := range ents {
		if ent.IsInfo() {
			if err := bInfos.Put(stagedInfoKey(p), ent.Info.Marshal(nil)); err != nil {
				return err
			}
			continue
		}
		if err := putExtent(bExt, p, stagedExtentHash, ent.EndAt(), ent.Extent); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cache) DeleteStagedEntries(ctx context.Context, p string) error {
	p = CleanPath(p)
	if err := requireNonEmptyPath(p); err != nil {
		return err
	}
	return c.deleteStagedEntries(ctx, p)
}

func (c *Cache) HasStagedEntries(ctx context.Context, p string) (bool, error) {
	p = CleanPath(p)
	if err := requireNonEmptyPath(p); err != nil {
		return false, err
	}
	bInfos := c.tx.Bucket([]byte(bucketInfos))
	if bInfos != nil && bInfos.Get(stagedInfoKey(p)) != nil {
		return true, nil
	}
	bExt := c.tx.Bucket([]byte(bucketExtents))
	if bExt == nil {
		return false, nil
	}
	prefix := extentPrefix(p, stagedExtentHash)
	k, _ := bExt.Cursor().Seek(prefix)
	return k != nil && bytes.HasPrefix(k, prefix), nil
}

func (c *Cache) GetStagedEntries(ctx context.Context, p string, out []gotfs.Entry) ([]gotfs.Entry, error) {
	p = CleanPath(p)
	if err := requireNonEmptyPath(p); err != nil {
		return nil, err
	}
	bInfos := c.tx.Bucket([]byte(bucketInfos))
	if bInfos != nil {
		if data := bInfos.Get(stagedInfoKey(p)); len(data) > 0 {
			var info gotfs.Info
			if err := info.Unmarshal(data); err != nil {
				return nil, err
			}
			k, err := gotfs.NewInfoKey(p)
			if err != nil {
				return nil, err
			}
			out = append(out, gotfs.Entry{
				Key:   k,
				Value: gotfs.Value{Info: info},
			})
		}
	}
	bExt := c.tx.Bucket([]byte(bucketExtents))
	if bExt == nil {
		return out, nil
	}
	prefix := extentPrefix(p, stagedExtentHash)
	cur := bExt.Cursor()
	for k, v := cur.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = cur.Next() {
		ee, err := parseExtentEntry(k, v)
		if err != nil {
			return nil, err
		}
		out = append(out, gotfs.Entry{
			Key:   gotfs.NewExtentKey(p, ee.EndAt),
			Value: gotfs.Value{Extent: ee.Extent},
		})
	}
	return out, nil
}

func (c *Cache) AddExtents(ctx context.Context, p string, paramHash [32]byte, ents []gotfs.Entry) error {
	p = CleanPath(p)
	if err := requireNonEmptyPath(p); err != nil {
		return err
	}
	if err := c.ensureBuckets(c.tx); err != nil {
		return err
	}
	b := c.tx.Bucket([]byte(bucketExtents))
	for _, ent := range ents {
		if ent.IsInfo() {
			continue
		}
		if err := putExtent(b, p, paramHash, ent.EndAt(), ent.Extent); err != nil {
			return err
		}
	}
	return nil
}

// GetExtents gets extents for (p, paramHash) and appends them to out
func (c *Cache) GetExtents(ctx context.Context, p string, paramHash [32]byte, out []gotfs.Entry) ([]gotfs.Entry, error) {
	p = CleanPath(p)
	if err := requireNonEmptyPath(p); err != nil {
		return nil, err
	}
	b := c.tx.Bucket([]byte(bucketExtents))
	if b == nil {
		return nil, fmt.Errorf("no extents for path + paramHash")
	}
	prefix := extentPrefix(p, paramHash)
	cur := b.Cursor()
	for k, _ := cur.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = cur.Next() {
		ee, err := parseExtentEntry(k, b.Get(k))
		if err != nil {
			return nil, err
		}
		out = append(out, gotfs.Entry{
			Key:   gotfs.NewExtentKey(p, ee.EndAt),
			Value: gotfs.Value{Extent: ee.Extent},
		})
	}
	return out, nil
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
func (c *Cache) NewInfoIterator() *DBInfoIterator {
	return newDBInfoIterator(c)
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

func newDBInfoIterator(db *Cache) *DBInfoIterator {
	seq := func(yield func(InfoEntry, error) bool) {
			err := func() error {
				b := db.tx.Bucket([]byte(bucketInfos))
				if b == nil {
					return nil
				}
				c := b.Cursor()
				for k, v := c.First(); k != nil; k, v = c.Next() {
					if isStagedInfoKey(k) {
						continue
					}
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
		}()
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
