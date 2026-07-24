package staging

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"go.brendoncarroll.net/state/posixfs"
	"go.etcd.io/bbolt"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotfs/gotfsdelta"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/gotwc/internal/porting"
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

	dmach gotfsdelta.Machine
	imp   *porting.Importer
}

type Env struct {
	Tx        *bbolt.Tx
	VTx       volumes.Tx
	GotFS     *gotfs.Machine
	FS        posixfs.FS
	Filter    func(string) bool
	ParamHash *[32]byte
}

// New wraps a transaction to create a transaction on a Stage
// paramHash if not-nil, causes operations to error if it does not
// match the paramHash in the stage
func New(env Env) *Tx {
	var imp *porting.Importer
	if env.ParamHash != nil {
		c := porting.NewCache(env.Tx)
		ss := gotfs.RW{Metadata: env.VTx, Data: env.VTx}
		imp = porting.NewImporter(&c, env.GotFS, ss, *env.ParamHash)
	}
	return &Tx{
		env: env,

		dmach: gotfsdelta.NewMachine(env.GotFS, [32]byte{}),
		imp:   imp,
	}
}

func (tx *Tx) setup(ctx context.Context) error {
	if _, err := tx.env.Tx.CreateBucketIfNotExists(bucketStage); err != nil {
		return err
	}
	return nil
}

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
	return errors.Join(tx.env.VTx.Abort(ctx), tx.env.Tx.Rollback())
}

func (tx *Tx) Commit(ctx context.Context) error {
	// commit to volume first
	if err := tx.env.VTx.Commit(ctx); err != nil {
		return err
	}
	// then bolt iff that succeeded
	return tx.env.Tx.Commit()
}

// Add adds a path to the stage.
func (tx *Tx) Add(ctx context.Context, p string) error {
	_, err := tx.imp.ImportPath(ctx, tx.env.FS, p)
	if err != nil {
		return err
	}
	return tx.put(ctx, p)
}

func (tx *Tx) Rm(ctx context.Context, p string) error {
	return nil
}

// Put replaces a path at p with root
func (tx *Tx) PutRoot(ctx context.Context, p string, root gotfs.Root) error {
	return tx.put(ctx, p, root.Segment())
}

// PutInfo creates a root, which can be used to overwrite just the info.
func (tx *Tx) PutInfo(ctx context.Context, ms stores.RW, p string, info gotfs.Info) error {
	p = cleanPath(p)
	root, err := tx.env.GotFS.NewEmpty(ctx, ms, 0)
	if err != nil {
		return err
	}
	root, err = tx.env.GotFS.PutInfo(ctx, ms, root, p, &info)
	if err != nil {
		return err
	}
	return tx.put(ctx, p, root.Segment())
}

// Delete removes a file at p with root
func (tx *Tx) Delete(ctx context.Context, p string) error {
	if err := tx.setup(ctx); err != nil {
		return nil
	}
	p = cleanPath(p)
	if err := tx.CheckConflict(ctx, p); err != nil {
		return err
	}
	return tx.put(ctx, p, gotfs.Segment{})
}

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
	_, err := tx.env.Tx.CreateBucket(bucketStage)
	return err
}

func (tx *Tx) IsEmpty(ctx context.Context) (bool, error) {
	b := tx.env.Tx.Bucket(bucketStage)
	return b.Inspect().KeyN > 0, nil
}

// CreateDelta creates a new FS Delta from the contents of the stage, and returns it.
func (tx *Tx) CreateDelta(ctx context.Context, s stores.RW) (gotfsdelta.Delta, error) {
	dw := tx.dmach.NewDeltaWriter(s)
	if err := tx.ForEach(ctx, gotkv.TotalSpan(), func(e Entry) error {
		seg := tx.env.GotFS.ShiftOut(e.Segment, e.Path)
		return dw.Push(ctx, seg)
	}); err != nil {
		return gotfsdelta.Delta{}, err
	}
	return dw.Finish(ctx)
}

func (tx *Tx) Store() stores.RW {
	return tx.env.VTx
}

// Applied returns an iterator for changes in the stage applied to root.
func (tx *Tx) Applied(ctx context.Context, s stores.RO, base gotfs.Root) (gotfsdelta.Applied, error) {
	s2 := stores.NewOverlay(s, stores.NewMem())
	delta, err := tx.CreateDelta(ctx, s2)
	if err != nil {
		return gotfsdelta.Applied{}, err
	}
	return gotfsdelta.NewApplied(s, base, []gotfsdelta.Delta{delta}), nil
}

func cleanPath(p string) string {
	p = path.Clean(p)
	p = strings.Trim(p, "/")
	if p == "." {
		p = ""
	}
	return p
}
