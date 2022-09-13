package gotfs

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/stdctx/logctx"
	"github.com/pkg/errors"

	"github.com/gotvc/got/pkg/chunking"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotfs/gotlob"
	"github.com/gotvc/got/pkg/gotkv"
)

const (
	DefaultMaxBlobSize          = 1 << 21
	DefaultMinBlobSizeData      = 1 << 12
	DefaultMeanBlobSizeData     = 1 << 20
	DefaultMeanBlobSizeMetadata = 1 << 13
)

type Option func(o *Operator)

func WithSalt(salt *[32]byte) Option {
	return func(o *Operator) {
		o.salt = salt
	}
}

// WithMetaCacheSize sets the size of the cache for metadata
func WithMetaCacheSize(n int) Option {
	return func(o *Operator) {
		o.metaCacheSize = n
	}
}

// WithContentCacheSize sets the size of the cache for raw data
func WithContentCacheSize(n int) Option {
	return func(o *Operator) {
		o.rawCacheSize = n
	}
}

type Operator struct {
	maxBlobSize                                 int
	minSizeData, meanSizeData, meanSizeMetadata int
	salt                                        *[32]byte
	rawCacheSize, metaCacheSize                 int

	rawOp        gdat.Operator
	gotkv        gotkv.Operator
	chunkingSeed *[32]byte
	lob          gotlob.Operator
}

func NewOperator(opts ...Option) Operator {
	o := Operator{
		maxBlobSize:      DefaultMaxBlobSize,
		minSizeData:      DefaultMinBlobSizeData,
		meanSizeData:     DefaultMeanBlobSizeData,
		meanSizeMetadata: DefaultMeanBlobSizeMetadata,
		salt:             &[32]byte{},
		rawCacheSize:     8,
		metaCacheSize:    16,
	}
	for _, opt := range opts {
		opt(&o)
	}

	// data
	var rawSalt [32]byte
	gdat.DeriveKey(rawSalt[:], o.salt, []byte("raw"))
	o.rawOp = gdat.NewOperator(
		gdat.WithSalt(&rawSalt),
		gdat.WithCacheSize(o.rawCacheSize),
	)
	var chunkingSeed [32]byte
	gdat.DeriveKey(chunkingSeed[:], o.salt, []byte("chunking"))
	o.chunkingSeed = &chunkingSeed

	// metadata
	var metadataSalt [32]byte
	gdat.DeriveKey(metadataSalt[:], o.salt, []byte("gotkv"))
	metaOp := gdat.NewOperator(
		gdat.WithSalt(&metadataSalt),
		gdat.WithCacheSize(o.metaCacheSize),
	)
	var treeSeed [16]byte
	gdat.DeriveKey(treeSeed[:], o.salt, []byte("gotkv-seed"))
	o.gotkv = gotkv.NewOperator(
		o.meanSizeMetadata,
		o.maxBlobSize,
		gotkv.WithDataOperator(metaOp),
		gotkv.WithSeed(&treeSeed),
	)
	lobOpts := []gotlob.Option{
		gotlob.WithChunking(false, func(onChunk chunking.ChunkHandler) *chunking.ContentDefined {
			return chunking.NewContentDefined(o.minSizeData, o.meanSizeData, o.maxBlobSize, o.chunkingSeed, onChunk)
		}),
		gotlob.WithFilter(func(x []byte) bool {
			return isExtentKey(x)
		}),
	}
	o.lob = gotlob.NewOperator(&o.gotkv, &o.rawOp, lobOpts...)
	return o
}

func (o *Operator) MeanBlobSizeData() int {
	return o.meanSizeData
}

func (o *Operator) MeanBlobSizeMetadata() int {
	return o.meanSizeMetadata
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
	ent, err := o.gotkv.MaxEntry(ctx, ms, root, span)
	if err != nil {
		return "", nil, err
	}
	switch {
	case ent == nil:
		return "", nil, nil
	case isInfoKey(ent.Key):
		// found an info entry, parse it and return.
		p, err := parseInfoKey(ent.Key)
		if err != nil {
			return "", nil, err
		}
		info, err := parseInfo(ent.Value)
		if err != nil {
			return "", nil, err
		}
		return p, info, nil
	case isExtentKey(ent.Key):
		// found an extent key, use it's path to short cut to the info key.
		p, _, err := splitExtentKey(ent.Key)
		if err != nil {
			return "", nil, err
		}
		info, err := o.GetInfo(ctx, ms, root, p)
		return p, info, err
	default:
		return "", nil, fmt.Errorf("gotfs: found invalid entry %v", ent)
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
