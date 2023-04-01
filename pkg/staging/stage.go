package staging

import (
	"bytes"
	"context"
	"encoding/json"
	"path"
	"strings"

	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/stdctx/logctx"
	"github.com/pkg/errors"

	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/metrics"
)

type Storage interface {
	state.KVStore[[]byte, []byte]
}

type Operation struct {
	Delete *DeleteOp `json:"del,omitempty"`
	Put    *PutOp    `json:"put,omitempty"`
}

// DeleteOp deletes a path and everything beneath it
type DeleteOp struct{}

// PutOp replaces a path with a filesystem.
type PutOp gotfs.Root

type Stage struct {
	storage Storage
}

func New(stor Storage) *Stage {
	return &Stage{
		storage: stor,
	}
}

// Put replaces a path at p with root
func (s *Stage) Put(ctx context.Context, p string, root gotfs.Root) error {
	p = cleanPath(p)
	if err := s.CheckConflict(ctx, p); err != nil {
		return err
	}
	op := Operation{
		Put: (*PutOp)(&root),
	}
	return s.storage.Put(ctx, []byte(p), jsonMarshal(op))
}

// Delete removes a file at p with root
func (s *Stage) Delete(ctx context.Context, p string) error {
	p = cleanPath(p)
	if err := s.CheckConflict(ctx, p); err != nil {
		return err
	}
	fo := Operation{
		Delete: &DeleteOp{},
	}
	return s.storage.Put(ctx, []byte(p), jsonMarshal(fo))
}

func (s *Stage) Discard(ctx context.Context, p string) error {
	p = cleanPath(p)
	return s.storage.Delete(ctx, []byte(p))
}

// Get returns the operation, if any, staged for the path p
// If there is no operation staged Get returns (nil, nil)
func (s *Stage) Get(ctx context.Context, p string) (*Operation, error) {
	p = cleanPath(p)
	data, err := s.storage.Get(ctx, []byte(p))
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	var op Operation
	if err := json.Unmarshal(data, &op); err != nil {
		return nil, errors.Wrapf(err, "parsing staging operation: %q", data)
	}
	return &op, nil
}

func (s *Stage) ForEach(ctx context.Context, fn func(p string, op Operation) error) error {
	return state.ForEach[[]byte](ctx, s.storage, state.TotalSpan[[]byte](), func(k []byte) error {
		v, err := s.storage.Get(ctx, k)
		if err != nil {
			return err
		}
		var fo Operation
		if err := json.Unmarshal(v, &fo); err != nil {
			return errors.Wrapf(err, "parsing staging operation: %q", v)
		}
		p := string(k)
		return fn(p, fo)
	})
}

func (s *Stage) CheckConflict(ctx context.Context, p string) error {
	newError := func(p, conflictPath string) error {
		return errors.Errorf("cannot add %q to stage. conflicts with entry for %q", p, conflictPath)
	}
	p = cleanPath(p)
	// check for ancestors
	parts := strings.Split(p, "/")
	for i := len(parts) - 1; i > 0; i-- {
		conflictPath := strings.Join(parts[:i], "/")
		data, err := s.storage.Get(ctx, []byte(cleanPath(conflictPath)))
		if err != nil && !errors.Is(err, state.ErrNotFound) {
			return err
		}
		if len(data) > 0 {
			return newError(p, conflictPath)
		}
	}
	// check for descendents
	if err := state.ForEach[[]byte](ctx, s.storage, state.TotalSpan[[]byte](), func(k []byte) error {
		if bytes.HasPrefix(k, []byte(p+"/")) {
			return newError(p, string(k))
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (s *Stage) Reset(ctx context.Context) error {
	return state.ForEach[[]byte](ctx, s.storage, state.TotalSpan[[]byte](), func(k []byte) error {
		return s.storage.Delete(ctx, k)
	})
}

func (s *Stage) IsEmpty(ctx context.Context) (bool, error) {
	var keys [1][]byte
	n, err := s.storage.List(ctx, state.TotalSpan[[]byte](), keys[:])
	if err != nil {
		return false, err
	}
	return n == 0, nil
}

func (s *Stage) Apply(ctx context.Context, fsop *gotfs.Operator, ms, ds cadata.Store, base *gotfs.Root) (*gotfs.Root, error) {
	if base == nil {
		var err error
		base, err = fsop.NewEmpty(ctx, ms)
		if err != nil {
			return nil, err
		}
	}
	var segs []gotfs.Segment
	err := s.ForEach(ctx, func(p string, fileOp Operation) error {
		switch {
		case fileOp.Put != nil:
			var err error
			base, err = fsop.MkdirAll(ctx, ms, *base, path.Dir(p))
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
	root, err := fsop.Splice(ctx, ms, ds, segs)
	if err != nil {
		return nil, err
	}
	metrics.AddInt(ctx, "segs", len(segs), "segs")
	return root, nil
}

func jsonMarshal(x interface{}) []byte {
	data, err := json.Marshal(x)
	if err != nil {
		panic(err)
	}
	return data
}

func cleanPath(p string) string {
	p = path.Clean(p)
	p = strings.Trim(p, "/")
	if p == "." {
		p = ""
	}
	return p
}
