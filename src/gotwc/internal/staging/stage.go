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
	"sort"
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
	HasEntries bool
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
	if len(value) == 0 {
		return ent, nil
	}
	if err := ent.Segment.Unmarshal(value); err != nil {
		return Entry{}, err
	}
	return ent, nil
}

func marshalStagedValue(ent gotfs.Entry) ([]byte, error) {
	if ent.Key.IsInfo() {
		return ent.Info.Marshal(nil), nil
	}
	return ent.Extent.MarshalBinary()
}

func stageEntriesPrefix(p string) []byte {
	return append([]byte(p), 0)
}

func stageEntriesKey(p string, entryKey []byte) []byte {
	out := stageEntriesPrefix(p)
	out = append(out, entryKey...)
	return out
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

func (tx *Tx) stageState(ctx context.Context, p string) (staged bool, hasEntries bool, _ error) {
	b := tx.env.Tx.Bucket(bucketStage)
	if b == nil {
		return false, false, nil
	}
	if b.Get([]byte(p)) == nil {
		return false, false, nil
	}
	hasEntries, err := tx.c.HasStagedEntries(ctx, p)
	if err != nil {
		return false, false, err
	}
	return true, hasEntries, nil
}

func (tx *Tx) loadSegmentForPath(ctx context.Context, p string) (gotfs.Segment, error) {
	ents, err := tx.c.GetStagedEntries(ctx, p, nil)
	if err != nil {
		return gotfs.Segment{}, err
	}
	if len(ents) == 0 {
		return gotfs.Segment{Span: gotfs.SpanForPath(p)}, nil
	}
	kvmach := tx.env.GotFS.MetadataKV()
	kvb := kvmach.NewBuilder(tx.env.VolTx)
	for _, ent := range ents {
		k := ent.Key.Marshal(nil)
		v, err := marshalStagedValue(ent)
		if err != nil {
			return gotfs.Segment{}, err
		}
		if err := kvb.Put(ctx, k, v); err != nil {
			return gotfs.Segment{}, err
		}
	}
	root, err := kvb.Finish(ctx)
	if err != nil {
		return gotfs.Segment{}, err
	}
	return gotfs.Segment{Contents: root, Span: gotfs.SpanForPath(p)}, nil
}

// stagePath adds or replaces a path in the stage and stores its entries.
func (tx *Tx) stagePath(ctx context.Context, p string, ents []gotfs.Entry) error {
	if err := tx.setup(ctx); err != nil {
		return err
	}
	p = cleanPath(p)
	if p == "" {
		return fmt.Errorf("cannot stage empty path")
	}
	if err := tx.CheckConflict(ctx, p); err != nil {
		return err
	}
	if err := tx.c.ReplaceStagedEntries(ctx, p, ents); err != nil {
		return err
	}
	b := tx.env.Tx.Bucket(bucketStage)
	return b.Put([]byte(p), []byte{})
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
		if _, err := tx.c.UpdateInfo(ctx, p, info); err != nil {
			return err
		}
		if err := tx.c.SetKnown(ctx, p, &info); err != nil {
			return err
		}
		if err := tx.CheckConflict(ctx, p); err != nil {
			return err
		}
		ctx, cf := metrics.Child(ctx, p)
		defer cf()
		ents, err := tx.buildStageEntriesFromFSPath(ctx, fsys, p)
		if err != nil {
			return err
		}
		return tx.stagePath(ctx, p, ents)
	})
}

// Put replaces all files at or beneath p.
func (tx *Tx) Put(ctx context.Context, fsys posixfs.FS, p string) error {
	p = cleanPath(p)
	fi, err := fsys.Stat(p)
	if err != nil {
		if posixfs.IsErrNotExist(err) {
			if p == "" {
				return nil
			}
			return tx.stagePath(ctx, p, nil)
		}
		return err
	}
	stagedFiles := map[string]struct{}{}
	if fi.Mode().IsRegular() {
		finfo := porting.FileInfo{Mode: fi.Mode(), ModifiedAt: tai64.FromGoTime(fi.ModTime()), Size: fi.Size()}
		if _, err := tx.c.UpdateInfo(ctx, p, finfo); err != nil {
			return err
		}
		if err := tx.c.SetKnown(ctx, p, &finfo); err != nil {
			return err
		}
		ents, err := tx.buildStageEntriesFromFSPath(ctx, fsys, p)
		if err != nil {
			return err
		}
		if err := tx.stagePath(ctx, p, ents); err != nil {
			return err
		}
		stagedFiles[p] = struct{}{}
	} else {
		if err := streams.ForEach(ctx, porting.NewFSInfoIter(fsys, p), func(ent porting.InfoEntry) error {
			if ent.Info.Mode.IsDir() {
				return nil
			}
			if _, err := tx.c.UpdateInfo(ctx, ent.Path, ent.Info); err != nil {
				return err
			}
			if err := tx.c.SetKnown(ctx, ent.Path, &ent.Info); err != nil {
				return err
			}
			ents, err := tx.buildStageEntriesFromFSPath(ctx, fsys, ent.Path)
			if err != nil {
				return err
			}
			if err := tx.stagePath(ctx, ent.Path, ents); err != nil {
				return err
			}
			stagedFiles[ent.Path] = struct{}{}
			return nil
		}); err != nil {
			return err
		}
	}
	prefix := p
	if prefix != "" {
		prefix += "/"
	}
	it := tx.c.NewInfoIterator()
	return streams.ForEach(ctx, it, func(ent porting.InfoEntry) error {
		if ent.Info.Mode.IsDir() {
			return nil
		}
		if p != "" && ent.Path != p && !strings.HasPrefix(ent.Path, prefix) {
			return nil
		}
		if _, ok := stagedFiles[ent.Path]; ok {
			return nil
		}
		if _, err := fsys.Stat(ent.Path); err == nil {
			return nil
		} else if !posixfs.IsErrNotExist(err) {
			return err
		}
		return tx.stagePath(ctx, ent.Path, nil)
	})
}

func (tx *Tx) buildStageEntriesFromFSPath(ctx context.Context, fsys posixfs.FS, p string) ([]gotfs.Entry, error) {
	ss := gotfs.RW{Metadata: tx.env.VolTx, Data: tx.env.VolTx}
	root, err := tx.env.GotFS.NewEmpty(ctx, ss.Metadata, 0o755)
	if err != nil {
		return nil, err
	}
	var addPath func(string) error
	addPath = func(p2 string) error {
		finfo, err := fsys.Stat(p2)
		if err != nil {
			return err
		}
		if finfo.IsDir() {
			root, err = tx.env.GotFS.MkdirAll(ctx, ss.Metadata, root, p2)
			if err != nil {
				return err
			}
			dirents, err := posixfs.ReadDir(fsys, p2)
			if err != nil {
				return err
			}
			sort.Slice(dirents, func(i, j int) bool {
				return dirents[i].Name < dirents[j].Name
			})
			for _, dirent := range dirents {
				if err := addPath(path.Join(p2, dirent.Name)); err != nil {
					return err
				}
			}
			return nil
		}
		if !finfo.Mode().IsRegular() {
			return fmt.Errorf("unsupported file mode %v at %q", finfo.Mode(), p2)
		}
		parent := path.Dir(p2)
		if parent != "." && parent != "" {
			root, err = tx.env.GotFS.MkdirAll(ctx, ss.Metadata, root, parent)
			if err != nil {
				return err
			}
		}
		f, err := fsys.OpenFile(p2, posixfs.O_RDONLY, 0)
		if err != nil {
			return err
		}
		defer f.Close()
		root, err = tx.env.GotFS.PutFile(ctx, ss, root, p2, f)
		return err
	}
	if err := addPath(p); err != nil {
		return nil, err
	}
	it := tx.env.GotFS.NewIterator(tx.env.VolTx, root, gotfs.SpanForPath(p))
	var out []gotfs.Entry
	for {
		var ent gotfs.Entry
		if err := streams.NextUnit(ctx, &it, &ent); err != nil {
			if streams.IsEOS(err) {
				break
			}
			return nil, err
		}
		keyData := ent.Key.Marshal(nil)
		var keyCopy gotfs.Key
		if err := keyCopy.Unmarshal(keyData); err != nil {
			return nil, err
		}
		entryCopy := gotfs.Entry{Key: keyCopy}
		if ent.Key.IsInfo() {
			entryCopy.Value.Info = ent.Info
		} else {
			entryCopy.Value.Extent = ent.Extent
		}
		out = append(out, entryCopy)
	}
	return out, nil
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
	return tx.stagePath(ctx, p, nil)
}

// Discard removes any changes staged for p
func (tx *Tx) Discard(ctx context.Context, p string) error {
	if err := tx.setup(ctx); err != nil {
		return err
	}
	p = cleanPath(p)
	b := tx.env.Tx.Bucket(bucketStage)
	if b == nil {
		return nil
	}
	if p == "" {
		return tx.Clear(ctx)
	}
	prefix := []byte(p + "/")
	var deletes [][]byte
	c := b.Cursor()
	start := []byte(p)
	for k, _ := c.Seek(start); k != nil; k, _ = c.Next() {
		if bytes.Equal(k, start) {
			deletes = append(deletes, append([]byte{}, k...))
			continue
		}
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		deletes = append(deletes, append([]byte{}, k...))
	}
	for _, k := range deletes {
		if err := b.Delete(k); err != nil {
			return err
		}
		if err := tx.c.DeleteStagedEntries(ctx, string(k)); err != nil {
			return err
		}
	}
	return nil
}

// Get returns the operation, if any, staged for the path p
// If there is no operation staged Get returns (nil, nil)
func (tx *Tx) Get(ctx context.Context, p string, dst *gotfs.Segment) (bool, error) {
	p = cleanPath(p)
	staged, hasEntries, err := tx.stageState(ctx, p)
	if err != nil {
		return false, err
	}
	if !staged {
		return false, nil
	}
	if dst != nil {
		if hasEntries {
			seg, err := tx.loadSegmentForPath(ctx, p)
			if err != nil {
				return false, err
			}
			*dst = seg
		} else {
			*dst = gotfs.Segment{Span: gotfs.SpanForPath(p)}
		}
	}
	return true, nil
}

func (tx *Tx) ForEach(ctx context.Context, span gotkv.Span, fn func(Entry) error) error {
	b := tx.env.Tx.Bucket(bucketStage)
	if b == nil {
		return nil
	}
	c := b.Cursor()
	for key, _ := c.Seek(span.Begin); key != nil; key, _ = c.Next() {
		if span.End != nil && bytes.Compare(key, span.End) >= 0 {
			break
		}
		p := string(key)
		_, hasEntries, err := tx.stageState(ctx, p)
		if err != nil {
			return err
		}
		ent := Entry{Path: p, HasEntries: hasEntries}
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
		found, _, err := tx.stageState(ctx, k)
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
	b := tx.env.Tx.Bucket(bucketStage)
	if b != nil {
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if err := tx.c.DeleteStagedEntries(ctx, string(k)); err != nil {
				return err
			}
		}
	}
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
	if b == nil {
		return true, nil
	}
	return b.Inspect().KeyN == 0, nil
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
		if !e.HasEntries {
			changes = append(changes, gotfs.Segment{Span: gotfs.SpanForPath(e.Path)})
			return nil
		}
		seg, err := tx.loadSegmentForPath(ctx, e.Path)
		if err != nil {
			return err
		}
		changes = append(changes, seg)
		return nil
	}); err != nil {
		return gotfs.Root{}, err
	}
	if len(changes) == 0 {
		return base, nil
	}
	sort.Slice(changes, func(i, j int) bool {
		return bytes.Compare(changes[i].Span.Begin, changes[j].Span.Begin) < 0
	})
	parentSet := map[string]struct{}{}
	var parents []string
	for _, change := range changes {
		var k gotfs.Key
		if err := k.Unmarshal(change.Span.Begin); err != nil {
			continue
		}
		parent := path.Dir(k.Path())
		if parent == "." || parent == "" {
			continue
		}
		if _, exists := parentSet[parent]; exists {
			continue
		}
		parentSet[parent] = struct{}{}
		parents = append(parents, parent)
	}
	sort.Strings(parents)
	fn, err := fsvmmach.NewFunction(ctx, s2.Metadata, func(fb *gotfsvm.FnBuilder) (gotfsvm.Expr[gotfs.Root], error) {
		baseExpr := fb.Input(0)
		for _, p := range parents {
			baseExpr = fb.MkdirAll(baseExpr, p, 0o755)
		}
		return fb.Promote(fb.ChangesOnBase(baseExpr, changes)), nil
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
// if base is nil, then paths are not compared against a base filesystem.
func (tx *Tx) ForEachStaged(ctx context.Context, ss gotfs.RO, base *gotfs.Root, fn func(p string, op FileOperation) error) error {
	var root gotfs.Root
	hasBase := base != nil
	if hasBase {
		root = *base
	}
	return tx.ForEach(ctx, gotkv.Span{}, func(ent Entry) error {
		var op FileOperation
		switch {
		case !ent.HasEntries:
			// it's a delete
			op.Delete = &DeleteOp{}
		default:
			if !hasBase {
				op.Create = &CreateOp{}
				break
			}
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
	// Otherwise the file has been deleted from the working copy, but was previously in GotFS
	Exists     bool
	Mode       fs.FileMode
	ModifiedAt tai64.TAI64N
}

// ForEachDirty lists all of the files which are dirty.
// No information is returned for the untracked files.
// If a file is not in the stage, and is not in base, then it is considered untracked.
func (tx *Tx) ForEachDirty(ctx context.Context, fsys posixfs.FS, ss gotfs.RO, base gotfs.Root, fn func(df DirtyFile) error) error {
	if base.Ref.IsZero() {
		return nil
	}
	staged := map[string]struct{}{}
	if err := tx.ForEach(ctx, gotkv.TotalSpan(), func(ent Entry) error {
		staged[ent.Path] = struct{}{}
		return nil
	}); err != nil {
		return err
	}
	emitted := map[string]struct{}{}
	emit := func(df DirtyFile) error {
		if _, exists := emitted[df.Path]; exists {
			return nil
		}
		emitted[df.Path] = struct{}{}
		return fn(df)
	}
	isStaged := func(p string) bool {
		for {
			if _, ok := staged[p]; ok {
				return true
			}
			i := strings.LastIndexByte(p, '/')
			if i < 0 {
				break
			}
			p = p[:i]
		}
		return false
	}
	it := porting.NewFSInfoIter(fsys, "")
	if err := streams.ForEach(ctx, it, func(ent porting.InfoEntry) error {
		if ent.Info.Mode.IsDir() {
			return nil
		}
		if isStaged(ent.Path) {
			return nil
		}
		if tracked, err := tx.env.GotFS.Exists(ctx, ss.Metadata, base, ent.Path); err != nil {
			return err
		} else if !tracked {
			return nil
		}
		known, err := tx.c.IsKnown(ctx, ent.Path, ent.Info)
		if err != nil {
			return err
		}
		if known {
			return nil
		}
		return emit(DirtyFile{
			Path:       ent.Path,
			Exists:     true,
			Mode:       ent.Info.Mode,
			ModifiedAt: ent.Info.ModifiedAt,
		})
	}); err != nil {
		return err
	}

	var walkBaseFiles func(string) error
	walkBaseFiles = func(p string) error {
		return tx.env.GotFS.ReadDir(ctx, ss.Metadata, base, p, func(de gotfs.DirEnt) error {
			p2 := path.Join(p, de.Name)
			if de.Mode.IsDir() {
				return walkBaseFiles(p2)
			}
			if isStaged(p2) {
				return nil
			}
			finfo, err := fsys.Stat(p2)
			if err != nil {
				if posixfs.IsErrNotExist(err) {
					return emit(DirtyFile{Path: p2, Exists: false})
				}
				return err
			}
			known, err := tx.c.IsKnown(ctx, p2, porting.FileInfo{
				Mode:       finfo.Mode(),
				ModifiedAt: tai64.FromGoTime(finfo.ModTime()),
				Size:       finfo.Size(),
			})
			if err != nil {
				return err
			}
			if known {
				return nil
			}
			return emit(DirtyFile{
				Path:       p2,
				Exists:     true,
				Mode:       finfo.Mode(),
				ModifiedAt: tai64.FromGoTime(finfo.ModTime()),
			})
		})
	}
	if err := walkBaseFiles(""); err != nil {
		return err
	}
	return nil
}

// ForEachUntracked iterates over all the untracked files.
func (tx *Tx) ForEachUntracked(ctx context.Context, fsys posixfs.FS, ss gotfs.RO, base *gotfs.Root, fn func(p string) error) error {
	findStaged := func(p string) (bool, bool, error) {
		parts := strings.Split(p, "/")
		for i := len(parts); i > 0; i-- {
			p2 := strings.Join(parts[:i], "/")
			found, hasEntries, err := tx.stageState(ctx, p2)
			if err != nil {
				return false, false, err
			}
			if found {
				return true, hasEntries, nil
			}
		}
		return false, false, nil
	}
	it := porting.NewFSInfoIter(fsys, "")
	return streams.ForEach(ctx, it, func(ent porting.InfoEntry) error {
		if ent.Info.Mode.IsDir() {
			return nil
		}
		if hasStaged, tracked, err := findStaged(ent.Path); err != nil {
			return err
		} else if hasStaged {
			if tracked {
				return nil
			}
			return fn(ent.Path)
		}
		if base == nil {
			return fn(ent.Path)
		}
		tracked, err := tx.env.GotFS.Exists(ctx, ss.Metadata, *base, ent.Path)
		if err != nil {
			return err
		}
		if tracked {
			return nil
		}
		return fn(ent.Path)
	})
}

func cleanPath(p string) string {
	p = path.Clean(p)
	p = strings.Trim(p, "/")
	if p == "." {
		p = ""
	}
	return p
}
