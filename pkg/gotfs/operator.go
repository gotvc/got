package gotfs

import (
	"bytes"
	"context"
	"os"
	"strings"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotfs/gotlob"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/gotvc/got/pkg/logctx"
	"github.com/pkg/errors"
)

const (
	DefaultMaxBlobSize         = gotlob.DefaultMaxBlobSize
	DefaultMinBlobSizeData     = gotlob.DefaultMinBlobSizeData
	DefaultAverageBlobSizeData = gotlob.DefaultAverageBlobSizeData
	DefaultAverageBlobSizeInfo = gotlob.DefaultAverageBlobSizeKV
)

type Option func(o *Operator)

func WithSalt(salt *[32]byte) Option {
	return func(o *Operator) {
		o.lobOpts = append(o.lobOpts, gotlob.WithSalt(salt))
	}
}

// WithMetaCacheSize sets the size of the cache for metadata
func WithMetaCacheSize(n int) Option {
	return func(o *Operator) {
		o.lobOpts = append(o.lobOpts, gotlob.WithMetaCacheSize(n))
	}
}

// WithContentCacheSize sets the size of the cache for raw data
func WithContentCacheSize(n int) Option {
	return func(o *Operator) {
		o.lobOpts = append(o.lobOpts, gotlob.WithContentCacheSize(n))
	}
}

type Operator struct {
	lobOpts []gotlob.Option

	lob   gotlob.Operator
	gotkv *gotkv.Operator
}

func NewOperator(opts ...Option) Operator {
	o := Operator{}
	for _, opt := range opts {
		opt(&o)
	}
	o.lobOpts = append(o.lobOpts, gotlob.WithFilter(func(x []byte) bool {
		return isExtentKey(x)
	}))
	o.lob = gotlob.NewOperator(o.lobOpts...)
	o.gotkv = o.lob.GotKV()
	return o
}

// Select returns a new root containing everything under p, shifted to the root.
func (o *Operator) Select(ctx context.Context, s cadata.Store, root Root, p string) (*Root, error) {
	p = cleanPath(p)
	_, err := o.GetInfo(ctx, s, root, p)
	if err != nil {
		return nil, err
	}
	x := &root
	k := makeInfoKey(p)
	if x, err = o.deleteOutside(ctx, s, *x, gotkv.PrefixSpan(k)); err != nil {
		return nil, err
	}
	var prefix []byte
	if len(k) > 1 {
		prefix = k[:len(k)-1]
	}
	if x, err = o.gotkv.RemovePrefix(ctx, s, *x, prefix); err != nil {
		return nil, err
	}
	return x, err
}

func (o *Operator) deleteOutside(ctx context.Context, s cadata.Store, root Root, span gotkv.Span) (*Root, error) {
	x := &root
	var err error
	if x, err = o.gotkv.DeleteSpan(ctx, s, *x, gotkv.Span{Begin: nil, End: span.Begin}); err != nil {
		return nil, err
	}
	if x, err = o.gotkv.DeleteSpan(ctx, s, *x, gotkv.Span{Begin: span.End, End: nil}); err != nil {
		return nil, err
	}
	return x, err
}

func (o *Operator) ForEach(ctx context.Context, s cadata.Store, root Root, p string, fn func(p string, md *Info) error) error {
	p = cleanPath(p)
	fn2 := func(ent gotkv.Entry) error {
		if !isExtentKey(ent.Key) {
			md, err := parseInfo(ent.Value)
			if err != nil {
				return err
			}
			p, err := parseInfoKey(ent.Key)
			if err != nil {
				return err
			}
			return fn(p, md)
		}
		return nil
	}
	k := makeInfoKey(p)
	return o.gotkv.ForEach(ctx, s, root, gotkv.PrefixSpan(k), fn2)
}

// ForEachLeaf calls fn with each regular file in root, beneath p.
func (o *Operator) ForEachLeaf(ctx context.Context, s cadata.Store, root Root, p string, fn func(p string, md *Info) error) error {
	return o.ForEach(ctx, s, root, p, func(p string, md *Info) error {
		if os.FileMode(md.Mode).IsDir() {
			return nil
		}
		return fn(p, md)
	})
}

// Graft places branch at p in root.
// If p == "" then branch is returned unaltered.
func (o *Operator) Graft(ctx context.Context, ms, ds cadata.Store, root Root, p string, branch Root) (*Root, error) {
	p = cleanPath(p)
	if p == "" {
		return &branch, nil
	}
	root2, err := o.MkdirAll(ctx, ms, root, parentPath(p))
	if err != nil {
		return nil, err
	}
	k := makeInfoKey(p)
	branch2 := o.gotkv.AddPrefix(branch, k[:len(k)-1])
	return o.Splice(ctx, ms, ds, []Segment{
		{
			Span: gotkv.Span{Begin: nil, End: k},
			Root: *root2,
		},
		{
			Span: gotkv.TotalSpan(),
			Root: branch2,
		},
		{
			Span: gotkv.Span{Begin: gotkv.PrefixEnd(k), End: nil},
			Root: *root2,
		},
	})
}

func (o *Operator) AddPrefix(root Root, p string) Root {
	p = cleanPath(p)
	k := makeInfoKey(p)
	return o.gotkv.AddPrefix(root, k[:len(k)-1])
}

// MaxInfo returns the maximum path and the corresponding Info for the path.
// If no Info entry can be found MaxInfo returns ("", nil, nil)
func (o *Operator) MaxInfo(ctx context.Context, ms cadata.Store, root Root, span Span) (string, *Info, error) {
	for {
		ent, err := o.gotkv.MaxEntry(ctx, ms, root, span)
		if err != nil {
			return "", nil, err
		}
		if ent == nil {
			return "", nil, nil
		}
		// found an info entry, parse it and return.
		if isInfoKey(ent.Key) {
			p, err := parseInfoKey(ent.Key)
			if err != nil {
				return "", nil, err
			}
			info, err := parseInfo(ent.Value)
			if err != nil {
				return "", nil, err
			}
			return p, info, nil
		}
		// found an extent key, use it's path to short cut to the info key.
		if isExtentKey(ent.Key) {
			p, _, err := splitExtentKey(ent.Key)
			if err != nil {
				return "", nil, err
			}
			info, err := o.GetInfo(ctx, ms, root, p)
			return p, info, err
		}
		// need to keep going, update the span
		span.End = ent.Key
	}
}

func (o *Operator) Check(ctx context.Context, s Store, root Root, checkData func(ref gdat.Ref) error) error {
	var lastPath *string
	var lastOffset *uint64
	return o.gotkv.ForEach(ctx, s, root, gotkv.Span{}, func(ent gotkv.Entry) error {
		switch {
		case lastPath == nil:
			logctx.Infof(ctx, "checking root")
			if !bytes.Equal(ent.Key, []byte{Sep}) {
				logctx.Infof(ctx, "first key: %q", ent.Key)
				return errors.Errorf("filesystem is missing root")
			}
			p := ""
			lastPath = &p
		case !isExtentKey(ent.Key):
			p, err := parseInfoKey(ent.Key)
			if err != nil {
				return err
			}
			_, err = parseInfo(ent.Value)
			if err != nil {
				return err
			}
			logctx.Infof(ctx, "checking %q", p)
			if !strings.HasPrefix(*lastPath, parentPath(p)) {
				return errors.Errorf("path %s did not have parent", p)
			}
			lastPath = &p
			lastOffset = nil
		default:
			p, off, err := splitExtentKey(ent.Key)
			if err != nil {
				return err
			}
			ext, err := parseExtent(ent.Value)
			if err != nil {
				return err
			}
			if *lastPath != p {
				logctx.Errorf(ctx, "path=%v offset=%v ext=%v", p, off, ext)
				return errors.Errorf("part not proceeded by metadata")
			}
			if lastOffset != nil && off <= *lastOffset {
				return errors.Errorf("part offsets not monotonic")
			}
			if err := checkData(ext.Ref); err != nil {
				return err
			}
			lastPath = &p
			lastOffset = &off
		}
		return nil
	})
}

func (o *Operator) Splice(ctx context.Context, ms, ds Store, segs []Segment) (*Root, error) {
	b := o.NewBuilder(ctx, ms, ds)
	for _, seg := range segs {
		if err := b.copyFrom(ctx, seg.Root, seg.Span); err != nil {
			return nil, err
		}
	}
	return b.Finish()
}
