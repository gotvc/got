package staging

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"strings"

	"errors"

	"go.brendoncarroll.net/state"
	"go.brendoncarroll.net/state/kv"
	"go.brendoncarroll.net/stdctx/logctx"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/internal/metrics"
	"github.com/gotvc/got/src/internal/stores"
)

type Storage interface {
	kv.Store[string, Operation]
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
	return s.storage.Put(ctx, p, op)
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
	return s.storage.Put(ctx, p, fo)
}

func (s *Stage) Discard(ctx context.Context, p string) error {
	p = cleanPath(p)
	return s.storage.Delete(ctx, p)
}

// Get returns the operation, if any, staged for the path p
// If there is no operation staged Get returns (nil, nil)
func (s *Stage) Get(ctx context.Context, p string) (*Operation, error) {
	p = cleanPath(p)
	op, err := kv.Get(ctx, s.storage, p)
	if err != nil {
		if errors.Is(err, state.ErrNotFound[string]{Key: p}) {
			return nil, nil
		}
		return nil, err
	}
	return &op, nil
}

func (s *Stage) ForEach(ctx context.Context, fn func(p string, op Operation) error) error {
	return kv.ForEach(ctx, s.storage, state.TotalSpan[string](), func(k string) error {
		op, err := kv.Get(ctx, s.storage, k)
		if err != nil {
			return err
		}
		return fn(k, op)
	})
}

func (s *Stage) CheckConflict(ctx context.Context, p string) error {
	newError := func(p, conflictPath string) error {
		return fmt.Errorf("cannot add %q to stage. conflicts with entry for %q", p, conflictPath)
	}
	p = cleanPath(p)
	// check for ancestors
	parts := strings.Split(p, "/")
	for i := len(parts) - 1; i > 0; i-- {
		conflictPath := strings.Join(parts[:i], "/")
		k := cleanPath(conflictPath)
		op, err := kv.Get(ctx, s.storage, k)
		if err != nil && !state.IsErrNotFound[Operation](err) {
			return err
		}
		if op != (Operation{}) {
			return newError(p, conflictPath)
		}
	}
	// check for descendents
	if err := kv.ForEach(ctx, s.storage, state.TotalSpan[string](), func(k string) error {
		if bytes.HasPrefix([]byte(k), []byte(p+"/")) {
			return newError(p, k)
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (s *Stage) Reset(ctx context.Context) error {
	return kv.ForEach(ctx, s.storage, state.TotalSpan[string](), func(k string) error {
		return s.storage.Delete(ctx, k)
	})
}

func (s *Stage) IsEmpty(ctx context.Context) (bool, error) {
	var keys [1]string
	n, err := s.storage.List(ctx, state.TotalSpan[string](), keys[:])
	if err != nil {
		return false, err
	}
	return n == 0, nil
}

func (s *Stage) Apply(ctx context.Context, fsag *gotfs.Machine, ss [2]stores.RW, base *gotfs.Root) (*gotfs.Root, error) {
	if base == nil {
		var err error
		base, err = fsag.NewEmpty(ctx, ss[1])
		if err != nil {
			return nil, err
		}
	}
	var segs []gotfs.Segment
	err := s.ForEach(ctx, func(p string, fileOp Operation) error {
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

func cleanPath(p string) string {
	p = path.Clean(p)
	p = strings.Trim(p, "/")
	if p == "." {
		p = ""
	}
	return p
}
