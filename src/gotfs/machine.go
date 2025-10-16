package gotfs

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"

	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/stdctx/logctx"

	"github.com/gotvc/got/src/chunking"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotfs/gotlob"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
)

const (
	DefaultMaxBlobSize          = 1 << 21
	DefaultMinBlobSizeData      = 1 << 12
	DefaultMeanBlobSizeData     = 1 << 20
	DefaultMeanBlobSizeMetadata = 1 << 13
)

type Option func(a *Machine)

func WithSalt(salt *[32]byte) Option {
	return func(a *Machine) {
		a.salt = salt
	}
}

// WithMetaCacheSize sets the size of the cache for metadata
func WithMetaCacheSize(n int) Option {
	return func(a *Machine) {
		a.metaCacheSize = n
	}
}

// WithContentCacheSize sets the size of the cache for raw data
func WithContentCacheSize(n int) Option {
	return func(a *Machine) {
		a.rawCacheSize = n
	}
}

type Machine struct {
	maxBlobSize                                 int
	minSizeData, meanSizeData, meanSizeMetadata int
	salt                                        *[32]byte
	rawCacheSize, metaCacheSize                 int

	rawOp        *gdat.Machine
	gotkv        gotkv.Machine
	chunkingSeed *[32]byte
	lob          gotlob.Machine
}

func NewMachine(opts ...Option) *Machine {
	o := &Machine{
		maxBlobSize:      DefaultMaxBlobSize,
		minSizeData:      DefaultMinBlobSizeData,
		meanSizeData:     DefaultMeanBlobSizeData,
		meanSizeMetadata: DefaultMeanBlobSizeMetadata,
		salt:             &[32]byte{},
		rawCacheSize:     8,
		metaCacheSize:    16,
	}
	for _, opt := range opts {
		opt(o)
	}

	// data
	var rawSalt [32]byte
	gdat.DeriveKey(rawSalt[:], o.salt, []byte("raw"))
	o.rawOp = gdat.NewMachine(
		gdat.WithSalt(&rawSalt),
		gdat.WithCacheSize(o.rawCacheSize),
	)
	var chunkingSeed [32]byte
	gdat.DeriveKey(chunkingSeed[:], o.salt, []byte("chunking"))
	o.chunkingSeed = &chunkingSeed

	// metadata
	var metadataSalt [32]byte
	gdat.DeriveKey(metadataSalt[:], o.salt, []byte("gotkv"))
	metaOp := gdat.NewMachine(
		gdat.WithSalt(&metadataSalt),
		gdat.WithCacheSize(o.metaCacheSize),
	)
	var treeSeed [16]byte
	gdat.DeriveKey(treeSeed[:], o.salt, []byte("gotkv-seed"))
	o.gotkv = gotkv.NewMachine(
		o.meanSizeMetadata,
		o.maxBlobSize,
		gotkv.WithDataMachine(metaOp),
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
	o.lob = gotlob.NewMachine(&o.gotkv, o.rawOp, lobOpts...)
	return o
}

func (a *Machine) MeanBlobSizeData() int {
	return a.meanSizeData
}

func (a *Machine) MeanBlobSizeMetadata() int {
	return a.meanSizeMetadata
}

// Select returns a new root containing everything under p, shifted to the root.
func (a *Machine) Select(ctx context.Context, s cadata.Store, root Root, p string) (*Root, error) {
	p = cleanPath(p)
	_, err := a.GetInfo(ctx, s, root, p)
	if err != nil {
		return nil, err
	}
	x := root.toGotKV()
	k := makeInfoKey(p)
	if x, err = a.deleteOutside(ctx, s, *x, gotkv.PrefixSpan(k)); err != nil {
		return nil, err
	}
	var prefix []byte
	if len(k) > 1 {
		prefix = k[:len(k)-1]
	}
	y, err := a.gotkv.RemovePrefix(ctx, s, *x, prefix)
	if err != nil {
		return nil, err
	}
	return newRoot(y), err
}

func (a *Machine) deleteOutside(ctx context.Context, s cadata.Store, root gotkv.Root, span gotkv.Span) (*gotkv.Root, error) {
	x := &root
	var err error
	if x, err = a.gotkv.DeleteSpan(ctx, s, *x, gotkv.Span{Begin: nil, End: span.Begin}); err != nil {
		return nil, err
	}
	if x, err = a.gotkv.DeleteSpan(ctx, s, *x, gotkv.Span{Begin: span.End, End: nil}); err != nil {
		return nil, err
	}
	return x, err
}

func (a *Machine) ForEach(ctx context.Context, s cadata.Getter, root Root, p string, fn func(p string, md *Info) error) error {
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
	return a.gotkv.ForEach(ctx, s, *root.toGotKV(), gotkv.PrefixSpan(k), fn2)
}

// ForEachLeaf calls fn with each regular file in root, beneath p.
func (a *Machine) ForEachLeaf(ctx context.Context, s cadata.Getter, root Root, p string, fn func(p string, md *Info) error) error {
	return a.ForEach(ctx, s, root, p, func(p string, md *Info) error {
		if os.FileMode(md.Mode).IsDir() {
			return nil
		}
		return fn(p, md)
	})
}

// Graft places branch at p in root.
// If p == "" then branch is returned unaltered.
func (a *Machine) Graft(ctx context.Context, ss [2]stores.RW, root Root, p string, branch Root) (*Root, error) {
	p = cleanPath(p)
	if p == "" {
		return &branch, nil
	}
	root2, err := a.MkdirAll(ctx, ss[1], root, parentPath(p))
	if err != nil {
		return nil, err
	}
	k := makeInfoKey(p)
	return a.Splice(ctx, ss, []Segment{
		{
			Span: gotkv.Span{Begin: nil, End: k},
			Contents: Expr{
				Root: *root2,
			},
		},
		{
			Span: SpanForPath(p),
			Contents: Expr{
				Root:      branch,
				AddPrefix: p,
			},
		},
		{
			Span: gotkv.Span{Begin: gotkv.PrefixEnd(k), End: nil},
			Contents: Expr{
				Root: *root2,
			},
		},
	})
}

func (a *Machine) addPrefix(root Root, p string) gotkv.Root {
	p = cleanPath(p)
	k := makeInfoKey(p)
	root2 := a.gotkv.AddPrefix(*root.toGotKV(), k[:len(k)-1])
	return root2
}

// MaxInfo returns the maximum path and the corresponding Info for the path.
// If no Info entry can be found MaxInfo returns ("", nil, nil)
func (a *Machine) MaxInfo(ctx context.Context, ms stores.RW, root Root, span Span) (string, *Info, error) {
	return a.maxInfo(ctx, ms, root.ToGotKV(), span)
}

func (a *Machine) maxInfo(ctx context.Context, ms stores.RW, root gotkv.Root, span Span) (string, *Info, error) {
	ent, err := a.gotkv.MaxEntry(ctx, ms, root, span)
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
		info, err := a.getInfo(ctx, ms, root, p)
		return p, info, err
	default:
		return "", nil, fmt.Errorf("gotfs: found invalid entry %v", ent)
	}
}

func (a *Machine) Check(ctx context.Context, s stores.Reading, root Root, checkData func(ref gdat.Ref) error) error {
	var lastPath *string
	var lastOffset *uint64
	return a.gotkv.ForEach(ctx, s, *root.toGotKV(), gotkv.Span{}, func(ent gotkv.Entry) error {
		switch {
		case lastPath == nil:
			logctx.Infof(ctx, "checking root")
			if !bytes.Equal(ent.Key, []byte{Sep}) {
				logctx.Infof(ctx, "first key: %q", ent.Key)
				return fmt.Errorf("filesystem is missing root")
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
				return fmt.Errorf("path %s did not have parent", p)
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
				return fmt.Errorf("part not proceeded by metadata")
			}
			if lastOffset != nil && off <= *lastOffset {
				return fmt.Errorf("part offsets not monotonic")
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

func (a *Machine) Splice(ctx context.Context, ss [2]stores.RW, segs []Segment) (*Root, error) {
	b := a.NewBuilder(ctx, ss[1], ss[0])
	for i, seg := range segs {
		if i > 0 && bytes.Compare(segs[i-1].Span.End, segs[i].Span.Begin) > 0 {
			return nil, fmt.Errorf("segs out of order, %d end=%q %d begin=%q", i-1, segs[i-1].Span.End, i, segs[i].Span.Begin)
		}

		var root gotkv.Root
		if seg.Contents.Root.Ref.IsZero() {
			r, err := a.gotkv.NewEmpty(ctx, ss[1])
			if err != nil {
				return nil, err
			}
			root = *r
		} else {
			if seg.Contents.AddPrefix != "" {
				root = a.addPrefix(seg.Contents.Root, seg.Contents.AddPrefix)
			} else {
				root = seg.Contents.Root.ToGotKV()
			}
		}
		if err := b.copyFrom(ctx, root, seg.Span); err != nil {
			return nil, err
		}
	}
	return b.Finish()
}
