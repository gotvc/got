package staging

import (
	"bytes"
	"context"
	"encoding/json"
	"path"
	"strings"

	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Storage interface {
	state.KVStore[[]byte, []byte]
}

type Operation struct {
	Delete bool        `json:"del,omitempty"`
	Put    *gotfs.Root `json:"put,omitempty"`
}

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
		Put: &root,
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
		Delete: true,
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
	emptyRoot, err := fsop.NewEmpty(ctx, ms)
	if err != nil {
		return nil, err
	}
	var segs []gotfs.Segment
	err = s.ForEach(ctx, func(p string, fileOp Operation) error {
		var pathRoot gotfs.Root
		switch {
		case fileOp.Put != nil:
			var err error
			base, err = fsop.MkdirAll(ctx, ms, *base, path.Dir(p))
			if err != nil {
				return err
			}
			pathRoot = *fileOp.Put
		case fileOp.Delete:
			pathRoot = *emptyRoot
		default:
			logrus.Warnf("empty op for path %q", p)
			return nil
		}
		segRoot := fsop.AddPrefix(pathRoot, p)
		segs = append(segs, gotfs.Segment{
			Root: segRoot,
			Span: gotfs.SpanForPath(p),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	segs = gotfs.ChangesOnBase(*base, segs)
	logrus.Println("splicing...")
	root, err := fsop.Splice(ctx, ms, ds, segs)
	if err != nil {
		return nil, err
	}
	logrus.Println("done splicing.")
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
