package staging

import (
	"context"
	"encoding/json"
	"path"
	"strings"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/sirupsen/logrus"
)

type Storage interface {
	Put(k, v []byte) error
	Get(k []byte) ([]byte, error)
	Delete(k []byte) error
	ForEach(fn func(k, v []byte) error) error
	DeleteAll() error
}

type FileOp struct {
	Delete bool        `json:"del,omitempty"`
	Put    *gotfs.Root `json:"put,omitempty"`
}

type Stage struct {
	storage Storage
	gotfs   *gotfs.Operator
	ms, ds  cadata.Store
}

func New(stor Storage, fsop *gotfs.Operator, ms, ds cadata.Store) *Stage {
	return &Stage{
		storage: stor,
		gotfs:   fsop,
		ms:      ms,
		ds:      ds,
	}
}

// Put replaces a file at p with root
func (s *Stage) Put(ctx context.Context, p string, root gotfs.Root) error {
	p = cleanPath(p)
	fo := FileOp{
		Put: &root,
	}
	return s.storage.Put([]byte(p), jsonMarshal(fo))
}

// Delete removes a file at p with root
func (s *Stage) Delete(ctx context.Context, p string) error {
	p = cleanPath(p)
	fo := FileOp{
		Delete: true,
	}
	return s.storage.Put([]byte(p), jsonMarshal(fo))
}

func (s *Stage) Untrack(ctx context.Context, p string) error {
	p = cleanPath(p)
	return s.storage.Delete([]byte(p))
}

func (s *Stage) ForEach(ctx context.Context, fn func(p string, op FileOp) error) error {
	return s.storage.ForEach(func(k, v []byte) error {
		p := string(k)
		var fo FileOp
		if err := json.Unmarshal(v, &fo); err != nil {
			return err
		}
		return fn(p, fo)
	})
}

func (s *Stage) Reset() error {
	return s.storage.DeleteAll()
}

func (s *Stage) Apply(ctx context.Context, base *gotfs.Root) (*gotfs.Root, error) {
	if base == nil {
		var err error
		base, err = s.gotfs.NewEmpty(ctx, s.ms)
		if err != nil {
			return nil, err
		}
	}
	emptyRoot, err := s.gotfs.NewEmpty(ctx, s.ms)
	if err != nil {
		return nil, err
	}
	var segs []gotfs.Segment
	err = s.ForEach(ctx, func(p string, fileOp FileOp) error {
		var pathRoot gotfs.Root
		switch {
		case fileOp.Put != nil:
			var err error
			base, err = s.gotfs.MkdirAll(ctx, s.ms, *base, path.Dir(p))
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
		segRoot, err := s.gotfs.AddPrefix(ctx, s.ms, p, pathRoot)
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
	root, err := s.gotfs.Splice(ctx, s.ms, s.ds, segs)
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
	return strings.Trim(p, "/")
}
