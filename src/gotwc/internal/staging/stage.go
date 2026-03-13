package staging

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"

	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/stdctx/logctx"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotfsvm"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/volumes"
)

type Operation struct {
	Delete *DeleteOp `json:"del,omitempty"`
	Put    *PutOp    `json:"put,omitempty"`
}

// DeleteOp deletes a path and everything beneath it
type DeleteOp struct{}

// PutOp replaces a path with a filesystem.
type PutOp = gotfs.Root

type Entry struct {
	Path string    `json:"p"`
	Op   Operation `json:"op"`
}

// Tx is a transaction on a stage
type Tx struct {
	tx        volumes.Tx
	gotkv     gotkv.Machine
	paramHash *[32]byte
	s         stores.RW

	kvtx *gotkv.Tx
}

func DefaultGotKV() gotkv.Machine {
	return gotkv.NewMachine(gotkv.Params{MeanSize: 1 << 12, MaxSize: 1 << 18})
}

// New wraps a transaction to create a transaction on a Stage
// paramHash if not-nil, causes operations to error if it does not
// match the paramHash in the stage
func New(kvmach *gotkv.Machine, tx volumes.Tx, paramHash *[32]byte) *Tx {
	return &Tx{
		tx:        tx,
		gotkv:     *kvmach,
		paramHash: paramHash,
		s:         stores.NewMem(),
	}
}

// setup performs idempotent initialization and should be called before
// performing any operation.
func (tx *Tx) setup(ctx context.Context) error {
	if tx.kvtx != nil {
		return nil
	}
	var root []byte
	if err := tx.tx.Load(ctx, &root); err != nil {
		return err
	}
	var kvroot gotkv.Root
	var s stores.RW = tx.tx
	if len(root) > 0 {
		if len(root) < 32 {
			return fmt.Errorf("too short to be stage root")
		}
		if tx.paramHash != nil && !bytes.Equal(tx.paramHash[:], root[:32]) {
			return fmt.Errorf("stage paramHash must match %x vs %x", tx.paramHash[:], root[:32])
		}
		if err := kvroot.Unmarshal(root[32:]); err != nil {
			return err
		}
	} else {
		r, err := tx.gotkv.NewEmpty(ctx, s)
		if err != nil {
			// this is for the read only case
			s = stores.NewMem()
			r, err = tx.gotkv.NewEmpty(ctx, s)
			if err != nil {
				return err
			}
		}
		kvroot = r
	}
	tx.kvtx = tx.gotkv.NewTx(s, kvroot)
	return nil
}

func (tx *Tx) save(ctx context.Context) error {
	if err := tx.setup(ctx); err != nil {
		return err
	}
	if tx.paramHash == nil {
		return fmt.Errorf("param has must be set to write to stage")
	}
	root := *tx.paramHash
	next, err := tx.kvtx.Flush(ctx)
	if err != nil {
		return err
	}
	if err := tx.tx.Save(ctx, next.Marshal(root[:])); err != nil {
		return err
	}
	return nil
}

func (tx *Tx) Abort(ctx context.Context) error {
	return tx.tx.Abort(ctx)
}

func (tx *Tx) Commit(ctx context.Context) error {
	if err := tx.save(ctx); err != nil {
		return err
	}
	return tx.tx.Commit(ctx)
}

func (tx *Tx) put(ctx context.Context, p string, op Operation) error {
	if err := tx.setup(ctx); err != nil {
		return nil
	}
	p = cleanPath(p)
	if err := tx.CheckConflict(ctx, p); err != nil {
		return err
	}
	val, err := json.Marshal(op)
	if err != nil {
		return err
	}
	return tx.kvtx.Put(ctx, []byte(p), val)
}

// Put replaces a path at p with root
func (tx *Tx) PutRoot(ctx context.Context, p string, root gotfs.Root) error {
	op := Operation{
		Put: (*PutOp)(&root),
	}
	return tx.put(ctx, p, op)
}

// PutInfo creates a root, which can be used to overwrite just the info.
func PutInfo(ctx context.Context, fsmach *gotfs.Machine, ms stores.RW, p string, info gotfs.Info) (*gotfs.Root, error) {
	p = cleanPath(p)
	root, err := fsmach.NewEmpty(ctx, ms, 0)
	if err != nil {
		return nil, err
	}
	return fsmach.PutInfo(ctx, ms, *root, p, &info)
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
	fo := Operation{
		Delete: &DeleteOp{},
	}
	val, err := json.Marshal(fo)
	if err != nil {
		return err
	}
	return tx.kvtx.Put(ctx, []byte(p), val)
}

func (tx *Tx) Discard(ctx context.Context, p string) error {
	if err := tx.setup(ctx); err != nil {
		return err
	}
	p = cleanPath(p)
	if err := tx.kvtx.Delete(ctx, []byte(p)); err != nil {
		return err
	}
	// Also discard any changes to subpaths
	return tx.ForEach(ctx, func(e Entry) error {
		if strings.HasPrefix(e.Path, p+"/") {
			return tx.kvtx.Delete(ctx, []byte(e.Path))
		}
		return nil
	})
}

// Get returns the operation, if any, staged for the path p
// If there is no operation staged Get returns (nil, nil)
func (tx *Tx) Get(ctx context.Context, p string, dst *Operation) (bool, error) {
	if err := tx.setup(ctx); err != nil {
		return false, err
	}
	p = cleanPath(p)
	var val []byte
	if found, err := tx.kvtx.Get(ctx, []byte(p), &val); err != nil {
		return false, err
	} else if !found {
		return false, nil
	}
	var op Operation
	if err := json.Unmarshal(val, &op); err != nil {
		return false, err
	}
	return true, nil
}

func (tx *Tx) Iterate(ctx context.Context, span gotkv.Span) (*Iterator, error) {
	if err := tx.setup(ctx); err != nil {
		return nil, err
	}
	tx.kvtx.Flush(ctx)
	it := tx.kvtx.Iterate(ctx, span)
	return &Iterator{it: it}, nil
}

func (tx *Tx) ForEach(ctx context.Context, fn func(Entry) error) error {
	it, err := tx.Iterate(ctx, gotkv.TotalSpan())
	if err != nil {
		return err
	}
	return streams.ForEach(ctx, it, fn)
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
		var op Operation
		found, err := tx.Get(ctx, k, &op)
		if err != nil {
			return err
		}
		if found {
			return newError(p, conflictPath)
		}
	}
	it, err := tx.Iterate(ctx, gotkv.PrefixSpan([]byte(p+"/")))
	if err != nil {
		return err
	}
	// check for descendents
	if err := streams.ForEach(ctx, it, func(ent Entry) error {
		return newError(p, ent.Path)
	}); err != nil {
		return err
	}
	return nil
}

// Clear deletes all entries from the staging area
func (tx *Tx) Clear(ctx context.Context) error {
	if err := tx.tx.Save(ctx, []byte{}); err != nil {
		return err
	}
	tx.kvtx = nil
	return nil
}

func (tx *Tx) IsEmpty(ctx context.Context) (bool, error) {
	it, err := tx.Iterate(ctx, gotkv.TotalSpan())
	if err != nil {
		return false, err
	}
	if err := streams.NextUnit(ctx, it, &Entry{}); err == nil {
		return false, nil
	} else if streams.IsEOS(err) {
		return true, nil
	} else {
		return false, err
	}
}

func (tx *Tx) CreateFunction(ctx context.Context, fsag *gotfs.Machine, ss gotfs.RW) (gotfsvm.Function, error) {
	it, err := tx.Iterate(ctx, gotkv.TotalSpan())
	if err != nil {
		return gotfsvm.Function{}, err
	}
	vm := gotfsvm.New(fsag)
	return vm.NewFunction(ctx, ss.Metadata, func(fb *gotfsvm.FnBuilder) (gotfsvm.Expr[gotfs.Root], error) {
		baseExpr := fb.Input(0)
		var segs []gotfs.Segment
		err = streams.ForEach(ctx, it, func(ent Entry) error {
			fileOp := ent.Op
			p := ent.Path
			switch {
			case fileOp.Put != nil:
				baseExpr = fb.MkdirAll(baseExpr, path.Dir(p), 0o755)
				segs = append(segs, fsag.ShiftOut(gotfs.Root(*fileOp.Put).Segment(), p))
			case fileOp.Delete != nil:
				segs = append(segs, gotfs.Segment{
					Span: gotfs.SpanForPath(p),
				})
			default:
				logctx.Warnf(ctx, "empty op for path %q", p)
				return nil
			}
			return nil
		})
		if err != nil {
			return gotfsvm.Expr[gotfs.Root]{}, err
		}
		concatExpr := fb.ChangesOnBase(baseExpr, segs)
		return fb.Promote(concatExpr), nil
	})
}

func (tx *Tx) Store() stores.RW {
	return tx.tx
}

func cleanPath(p string) string {
	p = path.Clean(p)
	p = strings.Trim(p, "/")
	if p == "." {
		p = ""
	}
	return p
}

type Iterator struct {
	it streams.Iterator[gotkv.Entry]
}

func (it *Iterator) Next(ctx context.Context, dsts []Entry) (int, error) {
	dst := &dsts[0]
	var kvent gotkv.Entry
	if err := streams.NextUnit(ctx, it.it, &kvent); err != nil {
		return 0, err
	}
	if err := json.Unmarshal(kvent.Value, &dst.Op); err != nil {
		return 0, err
	}
	dst.Path = string(kvent.Key)
	return 1, nil
}
