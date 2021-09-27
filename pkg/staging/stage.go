package staging

import (
	"bytes"
	"context"
	"encoding/json"
	"path"
	"strings"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Storage interface {
	Put(k, v []byte) error
	Get(k []byte) ([]byte, error)
	Delete(k []byte) error
	ForEach(fn func(k, v []byte) error) error
	DeleteAll() error
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
	return s.storage.Put([]byte(p), jsonMarshal(op))
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
	return s.storage.Put([]byte(p), jsonMarshal(fo))
}

func (s *Stage) Discard(ctx context.Context, p string) error {
	p = cleanPath(p)
	return s.storage.Delete([]byte(p))
}

func (s *Stage) Get(ctx context.Context, p string) (*Operation, error) {
	p = cleanPath(p)
	data, err := s.storage.Get([]byte(p))
	if err != nil {
		return nil, err
	}
	if len(data) < 1 {
		return nil, nil
	}
	var op Operation
	if err := json.Unmarshal(data, &op); err != nil {
		return nil, err
	}
	return &op, nil
}

func (s *Stage) ForEach(ctx context.Context, fn func(p string, op Operation) error) error {
	return s.storage.ForEach(func(k, v []byte) error {
		p := string(k)
		var fo Operation
		if err := json.Unmarshal(v, &fo); err != nil {
			return err
		}
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
		data, err := s.storage.Get([]byte(cleanPath(conflictPath)))
		if err != nil {
			return err
		}
		if len(data) > 0 {
			return newError(p, conflictPath)
		}
	}
	// check for descendents
	if err := s.storage.ForEach(func(k, _ []byte) error {
		if bytes.HasPrefix(k, []byte(p+"/")) {
			return newError(p, string(k))
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (s *Stage) Reset() error {
	return s.storage.DeleteAll()
}

func (s *Stage) IsEmpty(ctx context.Context) (bool, error) {
	var count int
	err := s.storage.ForEach(func(_, _ []byte) error {
		count++
		return nil
	})
	return count == 0, err
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
		segRoot, err := fsop.AddPrefix(ctx, ms, p, pathRoot)
		if err != nil {
			return err
		}
		segs = append(segs, gotfs.Segment{
			Root: *segRoot,
			Span: gotfs.SpanForPath(p),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	segs = prepareChanges(*base, segs)
	logrus.Println("splicing...")
	root, err := fsop.Splice(ctx, ms, ds, segs)
	if err != nil {
		return nil, err
	}
	logrus.Println("done splicing.")
	return root, nil
}

// prepareChanges ensures that the segments represent the whole key space, using base to fill in any gaps.
func prepareChanges(base gotfs.Root, changes []gotfs.Segment) []gotfs.Segment {
	var segs []gotfs.Segment
	for i := range changes {
		// create the span to reference the root, should be inbetween the two entries from segs
		var baseSpan gotkv.Span
		if i > 0 {
			baseSpan.Start = segs[len(segs)-1].Span.End
		}
		baseSpan.End = changes[i].Span.Start
		baseSeg := gotfs.Segment{Root: base, Span: baseSpan}

		segs = append(segs, baseSeg)
		segs = append(segs, changes[i])
	}
	if len(segs) > 0 {
		segs = append(segs, gotfs.Segment{
			Root: base,
			Span: gotkv.Span{
				Start: segs[len(segs)-1].Span.End,
				End:   nil,
			},
		})
	}
	return segs
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
