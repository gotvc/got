package staging

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"

	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/state"
	"go.brendoncarroll.net/stdctx/logctx"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/metrics"
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
	return gotkv.NewMachine(1<<12, 1<<18)
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
		kvroot = *r
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

// Put replaces a path at p with root
func (tx *Tx) Put(ctx context.Context, p string, root gotfs.Root) error {
	if err := tx.setup(ctx); err != nil {
		return nil
	}
	p = cleanPath(p)
	if err := tx.CheckConflict(ctx, p); err != nil {
		return err
	}
	op := Operation{
		Put: (*PutOp)(&root),
	}
	val, err := json.Marshal(op)
	if err != nil {
		return err
	}
	return tx.kvtx.Put(ctx, []byte(p), val)
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
	return tx.kvtx.Delete(ctx, []byte(p))
}

// Get returns the operation, if any, staged for the path p
// If there is no operation staged Get returns (nil, nil)
func (tx *Tx) Get(ctx context.Context, p string) (*Operation, error) {
	if err := tx.setup(ctx); err != nil {
		return nil, err
	}
	p = cleanPath(p)
	var val []byte
	if found, err := tx.kvtx.Get(ctx, []byte(p), &val); err != nil {
		return nil, err
	} else if !found {
		return nil, nil
	}
	var op Operation
	if err := json.Unmarshal(val, &op); err != nil {
		return nil, err
	}
	return &op, nil
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
		op, err := tx.Get(ctx, k)
		if err != nil && !state.IsErrNotFound[string](err) {
			return err
		}
		if op != nil {
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

func (tx *Tx) Apply(ctx context.Context, fsag *gotfs.Machine, ss [2]stores.RW, base *gotfs.Root) (*gotfs.Root, error) {
	if base == nil {
		var err error
		base, err = fsag.NewEmpty(ctx, ss[1])
		if err != nil {
			return nil, err
		}
	}
	it, err := tx.Iterate(ctx, gotkv.TotalSpan())
	if err != nil {
		return nil, err
	}
	var segs []gotfs.Segment
	err = streams.ForEach(ctx, it, func(ent Entry) error {
		fileOp := ent.Op
		p := ent.Path
		switch {
		case fileOp.Put != nil:
			var err error
			base, err = fsag.MkdirAll(ctx, ss[1], *base, path.Dir(p))
			if err != nil {
				return err
			}
			segs = append(segs, gotfs.Segment{
				Span: gotfs.SpanForPath(p),
				Contents: gotfs.Expr{
					Root:      gotfs.Root(*fileOp.Put),
					AddPrefix: p,
				},
			})
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
		return nil, err
	}
	segs = gotfs.ChangesOnBase(*base, segs)
	ctx, cf := metrics.Child(ctx, "splicing")
	defer cf()
	metrics.SetDenom(ctx, "segs", len(segs), "segs")
	root, err := fsag.Splice(ctx, ss, segs)
	if err != nil {
		return nil, err
	}
	metrics.AddInt(ctx, "segs", len(segs), "segs")
	return root, nil
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
