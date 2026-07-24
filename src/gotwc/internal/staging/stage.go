// Package staging implements a staging area for transactions on GotFS filesystems.
// The purpose of the stage is to accumulate changes made by the user using the got CLI,
// and to then apply them to a previous filesystem, to get a new one.
package staging

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strings"

	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/state/posixfs"
	"go.brendoncarroll.net/tai64"
	"go.etcd.io/bbolt"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotfsvm"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/gotwc/internal/porting"
	"github.com/gotvc/got/src/internal/metrics"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/volumes"
)

// Entry is an entry in the stage
type Entry struct {
	Path    string
	Segment gotfs.Segment
}

func (ent Entry) Key(out []byte) []byte {
	return append(out, ent.Path...)
}

func (ent Entry) Value(out []byte) []byte {
	return ent.Segment.Marshal(out)
}

func ParseEntry(key, value []byte) (Entry, error) {
	ent := Entry{
		Path: string(key),
	}
	if err := ent.Segment.Unmarshal(value); err != nil {
		return Entry{}, err
	}
	return ent, nil
}

var (
	bucketStage = []byte("stage")
)

// Tx is a transaction on a stage.
// It is not safe for concurrent use.
type Tx struct {
	env Env

	c   *porting.Cache
	imp *porting.Importer
}

type Env struct {
	// Tx is an open transaction on the WC db
	Tx *bbolt.Tx
	// VolTx is a transaction on the staging Volume.
	// All filesystem content is written here.
	VolTx volumes.Tx
	// GotFS is the gotfs machine to use for manipulating files.
	GotFS *gotfs.Machine
	// ParamHash if not-nil is the hash of the parameters that affect how files are converted to blobs
	// if nil, then the stage is read-only.
	ParamHash *[32]byte
}

// New wraps a transaction to create a transaction on a Stage
// paramHash if not-nil, causes operations to error if it does not
// match the paramHash in the stage
func New(env Env) *Tx {
	c := porting.NewCache(env.Tx)
	var imp *porting.Importer
	if env.ParamHash != nil {
		ss := gotfs.RW{Metadata: env.VolTx, Data: env.VolTx}
		imp = porting.NewImporter(&c, env.GotFS, ss, *env.ParamHash)
	}
	return &Tx{
		env: env,

		c:   &c,
		imp: imp,
	}
}

func (tx *Tx) Cache() *porting.Cache {
	return tx.c
}

// setup ensures that the needed buckets exist and that the paramHash matches the staging volume.
func (tx *Tx) setup(ctx context.Context) error {
	if _, err := tx.env.Tx.CreateBucketIfNotExists(bucketStage); err != nil {
		return err
	}
	var root []byte
	if err := tx.env.VolTx.Load(ctx, &root); err != nil {
		return err
	}
	if tx.env.ParamHash == nil {
		return nil
	}
	if len(root) > 0 {
		var paramHash [32]byte
		copy(paramHash[:], root)
		if paramHash != *tx.env.ParamHash {
			return fmt.Errorf("staging volume has wrong parameters %x vs. %x", paramHash, *tx.env.ParamHash)
		}
	}
	return tx.env.VolTx.Save(ctx, tx.env.ParamHash[:])
}

// put adds a path to the stage.
func (tx *Tx) put(ctx context.Context, p string) error {
	if err := tx.setup(ctx); err != nil {
		return err
	}
	p = cleanPath(p)
	if err := tx.CheckConflict(ctx, p); err != nil {
		return err
	}
	b := tx.env.Tx.Bucket(bucketStage)
	return b.Put([]byte(p), nil)
}

func (tx *Tx) Abort(ctx context.Context) error {
	return errors.Join(tx.env.VolTx.Abort(ctx), tx.env.Tx.Rollback())
}

// Commit commits the transaction to blobcache
func (tx *Tx) Commit(ctx context.Context) error {
	// commit to volume first
	if err := tx.env.VolTx.Commit(ctx); err != nil {
		return err
	}
	// then bolt iff that succeeded
	return tx.env.Tx.Commit()
}

// Add adds each file at or beneath p in the filesystem, individually.
// The filesystem is walked and each file added, will be added as it's own segment.
func (tx *Tx) Add(ctx context.Context, fsys posixfs.FS, p string) error {
	p = cleanPath(p)

	it := porting.NewFSInfoIter(fsys, p)
	return streams.ForEach(ctx, it, func(ent porting.InfoEntry) error {
		info := ent.Info
		p := ent.Path
		if info.Mode.IsDir() {
			// TODO, this should set the mode on the directory
			return nil
		}
		if err := tx.CheckConflict(ctx, p); err != nil {
			return err
		}
		ctx, cf := metrics.Child(ctx, p)
		defer cf()
		_, err := tx.imp.ImportPath(ctx, fsys, p)
		if err != nil {
			return err
		}
		return tx.put(ctx, p)
	})
}

// Put replaces all files at or beneath p.
func (tx *Tx) Put(ctx context.Context, fsys posixfs.FS, p string) error {
	p = cleanPath(p)
	_, err := tx.imp.ImportPath(ctx, fsys, p)
	if err != nil {
		return err
	}
	return tx.put(ctx, p)
}

// Delete removes all files at or beneath p in base.
// Delete reads from the gotfs.Root, not the local filesystem.
func (tx *Tx) Delete(ctx context.Context, fsys posixfs.FS, ss gotfs.RO, base gotfs.Root, p string) error {
	p = cleanPath(p)
	// do not allow deletion of a file which still exists on disk.
	// TODO: maybe the behavior should match git, and we should do the deletion here
	// if the file matches what is in base.
	if _, err := fsys.Stat(p); err != nil && !posixfs.IsErrNotExist(err) {
		return err
	} else if err == nil {
		return fmt.Errorf("cannot stage rm, file exists at path %s", p)
	}
	// TODO: delete from stage, then add path to store.
	return tx.put(ctx, p)
}

// Discard removes any changes staged for p
func (tx *Tx) Discard(ctx context.Context, p string) error {
	if err := tx.setup(ctx); err != nil {
		return err
	}
	p = cleanPath(p)
	b := tx.env.Tx.Bucket(bucketStage)
	if err := b.Delete([]byte(p)); err != nil {
		return err
	}
	// Also discard any changes to subpaths
	span := gotkv.PrefixSpan([]byte(p))
	prefix := []byte(p + "/")
	c := b.Cursor()
	for k, _ := c.Seek([]byte(p)); k != nil; k, _ = c.Next() {
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		if err := b.Delete(k); err != nil {
			return err
		}
	}
	return tx.ForEach(ctx, span, func(e Entry) error {
		if strings.HasPrefix(e.Path, p+"/") {
			return b.Delete([]byte(e.Path))
		}
		return nil
	})
}

// Get returns the operation, if any, staged for the path p
// If there is no operation staged Get returns (nil, nil)
func (tx *Tx) Get(ctx context.Context, p string, dst *gotfs.Segment) (bool, error) {
	p = cleanPath(p)
	b := tx.env.Tx.Bucket(bucketStage)
	if b == nil {
		return false, nil
	}
	val := b.Get([]byte(p))
	if val == nil {
		return false, nil
	}
	var seg gotfs.Segment
	if err := seg.Unmarshal(val); err != nil {
		return false, err
	}
	return true, nil
}

func (tx *Tx) ForEach(ctx context.Context, span gotkv.Span, fn func(Entry) error) error {
	b := tx.env.Tx.Bucket(bucketStage)
	if b == nil {
		return nil
	}
	c := b.Cursor()
	for key, value := c.First(); key != nil; key, value = c.Next() {
		ent, err := ParseEntry(key, value)
		if err != nil {
			return err
		}
		if err := fn(ent); err != nil {
			return err
		}
	}
	return nil
}

func (tx *Tx) CheckConflict(ctx context.Context, p string) error {
	newError := func(p, conflictPath string) error {
		return fmt.Errorf("cannot add %q to stage. conflicts with entry for %q", p, conflictPath)
	}
	p = cleanPath(p)
	// check for ancestors
	parts := strings.Split(p, "/")
	for i := len(parts) - 1; i > 0; i-- {
		conflictPath := strings.Join(parts[:i], "/")
		k := cleanPath(conflictPath)
		var seg gotfs.Segment
		found, err := tx.Get(ctx, k, &seg)
		if err != nil {
			return err
		}
		if found {
			return newError(p, conflictPath)
		}
	}
	// check for descendents
	span := gotkv.PrefixSpan([]byte(p + "/"))
	if err := tx.ForEach(ctx, span, func(ent Entry) error {
		return newError(p, ent.Path)
	}); err != nil {
		return err
	}
	return nil
}

// Clear deletes all entries from the staging area
func (tx *Tx) Clear(ctx context.Context) error {
	if err := tx.env.Tx.DeleteBucket(bucketStage); err != nil {
		return err
	}
	if _, err := tx.env.Tx.CreateBucket(bucketStage); err != nil {
		return err
	}
	return nil
}

// IsEmpty returns true if the stage is empty
func (tx *Tx) IsEmpty(ctx context.Context) (bool, error) {
	b := tx.env.Tx.Bucket(bucketStage)
	return b.Inspect().KeyN > 0, nil
}

func (tx *Tx) Store() stores.RW {
	return tx.env.VolTx
}

// overlay creates a RW by overlaying the stage volume, over the read-only ss.
// ss should be the space volume.
func (tx *Tx) overlay(ss gotfs.RO) gotfs.RW {
	return gotfs.RW{
		Data:     stores.NewOverlay(ss.Data, tx.env.VolTx),
		Metadata: stores.NewOverlay(ss.Metadata, tx.env.VolTx),
	}
}

func (tx *Tx) InitialFS(ctx context.Context, ss gotfs.RO) (gotfs.Root, error) {
	s2 := tx.overlay(ss)
	base, err := tx.env.GotFS.NewEmpty(ctx, s2.Metadata, 0o755)
	if err != nil {
		return gotfs.Root{}, err
	}
	return tx.Apply(ctx, ss, base)
}

// Apply applies the changes to the root and returns them.
// ss should contain all the data referenced by base.
// ss will not be written to during Apply.
func (tx *Tx) Apply(ctx context.Context, ss gotfs.RO, base gotfs.Root) (gotfs.Root, error) {
	fsvmmach := gotfsvm.New(tx.env.GotFS)
	s2 := gotfs.RW{
		Data:     stores.NewOverlay(ss.Data, tx.env.VolTx),
		Metadata: stores.NewOverlay(ss.Metadata, tx.env.VolTx),
	}
	var changes []gotfs.Segment
	if err := tx.ForEach(ctx, gotkv.TotalSpan(), func(e Entry) error {
		return nil
	}); err != nil {
		return gotfs.Root{}, err
	}
	fn, err := fsvmmach.NewFunction(ctx, s2.Metadata, func(fb *gotfsvm.FnBuilder) (gotfsvm.Expr[gotfs.Root], error) {
		base := fb.Input(0)
		return fb.Promote(fb.ChangesOnBase(base, changes)), nil
	})
	if err != nil {
		return gotfs.Root{}, err
	}
	return fsvmmach.Apply(ctx, s2, fn, []gotfsvm.Input{{Stores: s2.RO(), Root: base}})
}

type FileOperation struct {
	// Delete means the file was removed.
	Delete *DeleteOp
	Create *CreateOp
	Modify *ModifyOp
}

type DeleteOp struct{}

type CreateOp struct {
	// Mode fs.FileMode
}

type ModifyOp struct {
	// Mode fs.FileMode
}

// ForEachStaged lists all of the staged changes.
// if root is zero, then it will not be compared against.
func (tx *Tx) ForEachStaged(ctx context.Context, ss gotfs.RO, root gotfs.Root, fn func(p string, op FileOperation) error) error {
	return tx.ForEach(ctx, gotkv.Span{}, func(ent Entry) error {
		var op FileOperation
		switch {
		case ent.Segment.IsZero():
			// it's a delete
			op.Delete = &DeleteOp{}
		default:
			md, err := tx.env.GotFS.GetInfo(ctx, ss.Metadata, root, ent.Path)
			if err != nil && !posixfs.IsErrNotExist(err) {
				return err
			}
			if md == nil {
				op.Create = &CreateOp{}
			} else {
				op.Modify = &ModifyOp{}
			}
		}
		return fn(ent.Path, op)
	})
}

// DirtyFile is a file that has changed in the filesystem, but is not in the stage.
type DirtyFile struct {
	Path string

	// If true than the file exists in the working copy.
	Exists     bool
	Mode       fs.FileMode
	ModifiedAt tai64.TAI64N
}

// ForEachDirty lists all of the files which are dirty.
func (tx *Tx) ForEachDirty(ctx context.Context, fsys posixfs.FS, ss gotfs.RO, base gotfs.Root, fn func(df DirtyFile) error) error {
	if base.Ref.IsZero() {

	}
	// diff with base.
	return nil
}

func cleanPath(p string) string {
	p = path.Clean(p)
	p = strings.Trim(p, "/")
	if p == "." {
		p = ""
	}
	return p
}
