package gotfs

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/gdat"
	"github.com/brendoncarroll/got/pkg/gotkv"
	"github.com/pkg/errors"
)

type (
	Ref   = gotkv.Ref
	Store = gotkv.Store
	Root  = gotkv.Root
)

type Option func(o *Operator)

func WithDataOperator(dop gdat.Operator) Option {
	return func(o *Operator) {
		o.dop = dop
	}
}

type Operator struct {
	dop   gdat.Operator
	gotkv gotkv.Operator
}

func NewOperator(opts ...Option) Operator {
	o := Operator{
		dop: gdat.NewOperator(),
	}
	for _, opt := range opts {
		opt(&o)
	}
	o.gotkv = gotkv.NewOperator(gotkv.WithRefOperator(o.dop))
	return o
}

// Select returns a new root containing everything under p, shifted to the root.
func (o *Operator) Select(ctx context.Context, s cadata.Store, root Root, p string) (*Root, error) {
	p = cleanPath(p)
	_, err := o.GetMetadata(ctx, s, root, p)
	if err != nil {
		return nil, err
	}
	x := &root
	if x, err = o.deleteOutside(ctx, s, *x, gotkv.PrefixSpan([]byte(p))); err != nil {
		return nil, err
	}
	if x, err = o.gotkv.RemovePrefix(ctx, s, *x, []byte(p)); err != nil {
		return nil, err
	}
	return x, err
}

func (o *Operator) deleteOutside(ctx context.Context, s cadata.Store, root Root, span gotkv.Span) (*Root, error) {
	x := &root
	var err error
	if x, err = o.gotkv.DeleteSpan(ctx, s, *x, gotkv.Span{Start: nil, End: span.Start}); err != nil {
		return nil, err
	}
	if x, err = o.gotkv.DeleteSpan(ctx, s, *x, gotkv.Span{Start: span.End, End: nil}); err != nil {
		return nil, err
	}
	return x, err
}

func (o *Operator) ForEach(ctx context.Context, s cadata.Store, root Root, p string, fn func(p string, md *Metadata) error) error {
	p = cleanPath(p)
	fn2 := func(ent gotkv.Entry) error {
		if !isPartKey(ent.Key) {
			md, err := parseMetadata(ent.Value)
			if err != nil {
				return err
			}
			p := strings.Trim(string(ent.Key), string(Sep))
			return fn(p, md)
		}
		return nil
	}
	if err := o.gotkv.ForEach(ctx, s, root, gotkv.SingleKeySpan([]byte(p)), fn2); err != nil {
		return err
	}
	return o.gotkv.ForEach(ctx, s, root, gotkv.PrefixSpan([]byte(p+"/")), fn2)
}

func (o *Operator) ForEachFile(ctx context.Context, s cadata.Store, root Root, p string, fn func(p string, md *Metadata) error) error {
	return o.ForEach(ctx, s, root, p, func(p string, md *Metadata) error {
		if os.FileMode(md.Mode).IsDir() {
			return nil
		}
		return fn(p, md)
	})
}

// Graft places branch at p in root.
// If p == "" then branch is returned unaltered.
func (o *Operator) Graft(ctx context.Context, s cadata.Store, root Root, p string, branch Root) (*Root, error) {
	p = cleanPath(p)
	if p == "" {
		return &branch, nil
	}
	root2, err := o.MkdirAll(ctx, s, root, parentPath(p))
	if err != nil {
		return nil, err
	}
	b := o.gotkv.NewBuilder(s)
	beforeIt := o.gotkv.NewIterator(s, *root2, gotkv.Span{Start: nil, End: []byte(p)})
	branch2, err := o.gotkv.AddPrefix(ctx, s, branch, []byte(p))
	if err != nil {
		return nil, err
	}
	branchIt := o.gotkv.NewIterator(s, *branch2, gotkv.Span{})
	afterIt := o.gotkv.NewIterator(s, root, gotkv.Span{Start: gotkv.PrefixEnd([]byte(p)), End: nil})
	for _, it := range []gotkv.Iterator{beforeIt, branchIt, afterIt} {
		if err := gotkv.CopyAll(ctx, b, it); err != nil {
			return nil, err
		}
	}
	return b.Finish(ctx)
}

func (o *Operator) Check(ctx context.Context, s Store, root Root, checkData func(ref gdat.Ref) error) error {
	var lastPath *string
	var lastOffset *uint64
	return o.gotkv.ForEach(ctx, s, root, gotkv.Span{}, func(ent gotkv.Entry) error {
		switch {
		case lastPath == nil:
			log.Printf("checking root")
			if len(ent.Key) != 0 {
				return errors.Errorf("filesystem is missing root")
			}
			p := ""
			lastPath = &p
		case !isPartKey(ent.Key):
			_, err := parseMetadata(ent.Value)
			if err != nil {
				return err
			}
			p := string(ent.Key)
			log.Printf("checking %q", p)
			if !strings.HasPrefix(*lastPath, parentPath(p)) {
				return errors.Errorf("path %s did not have parent", p)
			}
			lastPath = &p
			lastOffset = nil
		default:
			p, off, err := splitPartKey(ent.Key)
			if err != nil {
				return err
			}
			part, err := parsePart(ent.Value)
			if err != nil {
				return err
			}
			if *lastPath != p {
				return errors.Errorf("part not proceeded by metadata")
			}
			if lastOffset != nil && off <= *lastOffset {
				return errors.Errorf("part offsets not monotonic")
			}
			ref, err := gdat.ParseRef(part.Ref)
			if err != nil {
				return err
			}
			if err := checkData(*ref); err != nil {
				return err
			}
			lastPath = &p
			lastOffset = &off
		}
		return nil
	})
}
