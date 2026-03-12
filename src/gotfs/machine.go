package gotfs

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"os"
	"slices"
	"strings"

	"go.brendoncarroll.net/exp/sbe"
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
	o.rawOp = gdat.NewMachine(gdat.Params{Salt: rawSalt, CacheSize: &o.rawCacheSize})
	var chunkingSeed [32]byte
	gdat.DeriveKey(chunkingSeed[:], o.salt, []byte("chunking"))
	o.chunkingSeed = &chunkingSeed

	// metadata
	var metadataSalt [32]byte
	gdat.DeriveKey(metadataSalt[:], o.salt, []byte("gotkv"))
	metaOp := gdat.NewMachine(gdat.Params{Salt: metadataSalt, CacheSize: &o.metaCacheSize})
	var treeSeed [16]byte
	gdat.DeriveKey(treeSeed[:], o.salt, []byte("gotkv-seed"))
	o.gotkv = gotkv.NewMachine(gotkv.Params{
		DataMach: metaOp,
		MeanSize: o.meanSizeMetadata,
		MaxSize:  o.maxBlobSize,
		Seed:     treeSeed,
	})
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

func (mach *Machine) MeanBlobSizeData() int {
	return mach.meanSizeData
}

func (mach *Machine) MeanBlobSizeMetadata() int {
	return mach.meanSizeMetadata
}

// MetadataKV returns the gotkv.Machine used for metadata
func (mach *Machine) MetadataKV() *gotkv.Machine {
	return &mach.gotkv
}

// Pick returns a new root containing everything under p, shifted to the root.
func (mach *Machine) Pick(ctx context.Context, s stores.RW, root Root, p string) (*Root, error) {
	p = cleanPath(p)
	_, err := mach.GetInfo(ctx, s, root, p)
	if err != nil {
		return nil, err
	}
	x := root.toGotKV()
	span := SpanForPath(p)
	if x, err = mach.deleteOutside(ctx, s, x, span); err != nil {
		return nil, err
	}
	if p == "" {
		return newRoot(x), nil
	}
	prefix := pathPrefixNoTrail(nil, p)
	y, err := mach.gotkv.RemovePrefix(ctx, s, x, prefix)
	if err != nil {
		return nil, err
	}
	return newRoot(y), err
}

func (mach *Machine) deleteOutside(ctx context.Context, s stores.RW, root gotkv.Root, span gotkv.Span) (gotkv.Root, error) {
	x := root
	var err error
	if x, err = mach.gotkv.DeleteSpan(ctx, s, x, gotkv.Span{Begin: nil, End: span.Begin}); err != nil {
		return gotkv.Root{}, err
	}
	if x, err = mach.gotkv.DeleteSpan(ctx, s, x, gotkv.Span{Begin: span.End, End: nil}); err != nil {
		return gotkv.Root{}, err
	}
	return x, err
}

func (mach *Machine) ForEach(ctx context.Context, s stores.Reading, root Root, p string, fn func(p string, md *Info) error) error {
	p = cleanPath(p)
	fn2 := func(ent gotkv.Entry) error {
		var key Key
		if err := key.Unmarshal(ent.Key); err != nil {
			return err
		}
		if key.IsInfo() {
			md, err := parseInfo(ent.Value)
			if err != nil {
				return err
			}
			return fn(p, md)
		}
		return nil
	}
	span := SpanForPath(p)
	return mach.gotkv.ForEach(ctx, s, root.toGotKV(), span, fn2)
}

// ForEachLeaf calls fn with each regular file in root, beneath p.
func (mach *Machine) ForEachLeaf(ctx context.Context, s stores.Reading, root Root, p string, fn func(p string, md *Info) error) error {
	return mach.ForEach(ctx, s, root, p, func(p string, md *Info) error {
		if os.FileMode(md.Mode).IsDir() {
			return nil
		}
		return fn(p, md)
	})
}

// Graft places branch at p in root.
// If p == "" then branch is returned unaltered.
func (mach *Machine) Graft(ctx context.Context, ss [2]stores.RW, root Root, p string, branch Root) (*Root, error) {
	p = cleanPath(p)
	if p == "" {
		return &branch, nil
	}
	root2, err := mach.MkdirAll(ctx, ss[1], root, parentPath(p))
	if err != nil {
		return nil, err
	}
	k := newInfoKey(p)
	return mach.Splice(ctx, ss, []Segment{
		{
			Span:     gotkv.Span{Begin: nil, End: k.Marshal(nil)},
			Contents: root2.ToGotKV(),
		},
		{
			Span:     SpanForPath(p),
			Contents: mach.addPrefix(branch, p),
		},
		{
			Span:     gotkv.Span{Begin: gotkv.PrefixEnd(k.Prefix(nil)), End: nil},
			Contents: root2.ToGotKV(),
		},
	})
}

func (mach *Machine) addPrefix(root Root, p string) gotkv.Root {
	prefix := pathPrefixNoTrail(nil, p)
	if len(prefix) == 0 {
		return root.ToGotKV()
	}
	root2 := mach.gotkv.AddPrefix(root.toGotKV(), prefix)
	return root2
}

// MaxInfo returns the maximum path and the corresponding Info for the path.
// If no Info entry can be found MaxInfo returns ("", nil, nil)
func (mach *Machine) MaxInfo(ctx context.Context, ms stores.Reading, root Root, span Span) (string, *Info, error) {
	return mach.maxInfo(ctx, ms, root.ToGotKV(), span)
}

func (mach *Machine) maxInfo(ctx context.Context, ms stores.Reading, root gotkv.Root, span Span) (string, *Info, error) {
	ent, err := mach.gotkv.MaxEntry(ctx, ms, root, span)
	if err != nil {
		return "", nil, err
	}
	if ent == nil {
		return "", nil, nil
	}
	var key Key
	if err := key.Unmarshal(ent.Key); err != nil {
		return "", nil, err
	}
	switch {
	case key.IsInfo():
		// found an info entry, parse it and return.
		p := key.Path()
		info, err := parseInfo(ent.Value)
		if err != nil {
			return "", nil, err
		}
		return p, info, nil
	case isExtentKey(ent.Key):
		// found an extent key, use it's path to short cut to the info key.
		p := key.Path()
		info, err := mach.getInfo(ctx, ms, root, p)
		return p, info, err
	default:
		return "", nil, fmt.Errorf("gotfs: found invalid entry %v", ent)
	}
}

var firstKey = newInfoKey("").Marshal(nil)

func (mach *Machine) Check(ctx context.Context, ms stores.Reading, root Root, checkData func(ref gdat.Ref) error) error {
	var lastPath *string
	var lastOffset *uint64
	return mach.gotkv.ForEach(ctx, ms, root.toGotKV(), gotkv.Span{}, func(ent gotkv.Entry) error {
		var key Key
		if err := key.Unmarshal(ent.Key); err != nil {
			return err
		}
		switch {
		case lastPath == nil:
			logctx.Infof(ctx, "checking root")
			if !bytes.Equal(ent.Key, firstKey) {
				logctx.Infof(ctx, "first key: %q", ent.Key)
				return fmt.Errorf("filesystem is missing root")
			}
			p := ""
			lastPath = &p
		case key.IsInfo():
			p := key.Path()
			if _, err := parseInfo(ent.Value); err != nil {
				return err
			}
			logctx.Infof(ctx, "checking %q", p)
			if !strings.HasPrefix(*lastPath, parentPath(p)) {
				return fmt.Errorf("path %s did not have parent", p)
			}
			lastPath = &p
			lastOffset = nil
		default:
			p := key.Path()
			endAt := key.EndAt()
			ext, err := parseExtent(ent.Value)
			if err != nil {
				return err
			}
			if *lastPath != p {
				logctx.Errorf(ctx, "path=%v endAt=%v ext=%v", p, endAt, ext)
				return fmt.Errorf("part not proceeded by metadata")
			}
			if lastOffset != nil && endAt <= *lastOffset {
				return fmt.Errorf("part offsets not monotonic")
			}
			if err := checkData(ext.Ref); err != nil {
				return err
			}
			lastPath = &p
			lastOffset = &endAt
		}
		return nil
	})
}

// Segment is a contiguous subset of a GotFS instance.
// It may not be a valid Root.
type Segment struct {
	// Contents is the gotkv instance representing the segment.
	// If it contains entries outside of Span, they will not be used.
	// If Contents is the zero value, then it will be interpretted as empty
	Contents gotkv.Root
	// Span is the span in the final Splice operation
	Span gotkv.Span
}

func (s Segment) String() string {
	return fmt.Sprintf("{ %v : %v}", s.Span, s.Contents.Ref)
}

func (s *Segment) Marshal(out []byte) []byte {
	out = sbe.AppendLP16(out, s.Contents.Marshal(nil))
	out = sbe.AppendLP16(out, s.Span.Marshal(nil))
	return out
}

func (s *Segment) Unmarshal(data []byte) error {
	contentData, data, err := sbe.ReadLP16(data)
	if err != nil {
		return err
	}
	if err := s.Contents.Unmarshal(contentData); err != nil {
		return err
	}
	spanData, _, err := sbe.ReadLP16(data)
	if err != nil {
		return err
	}
	return s.Span.Unmarshal(spanData)
}

// ShiftOut shifts all the entries in a segment out by path.
// A path at a/b/ in x will be at p + a/b/ in the returned segment.
func (mach *Machine) ShiftOut(x Segment, p string) Segment {
	prefix := pathPrefixNoTrail(nil, p)
	if len(prefix) == 0 {
		return x
	}
	newRoot := mach.gotkv.AddPrefix(x.Contents, prefix)
	return Segment{
		Span: gotkv.Span{
			Begin: slices.Concat(prefix, x.Span.Begin),
			End:   slices.Concat(prefix, x.Span.End),
		},
		Contents: newRoot,
	}
}

func (mach *Machine) Concat(ctx context.Context, ss [2]stores.RW, segs iter.Seq[Segment]) (Segment, error) {
	b := mach.NewBuilder(ctx, ss[1], ss[0])

	var i int
	var firstSeg, prevSeg Segment
	for seg := range segs {
		if i > 0 && bytes.Compare(prevSeg.Span.End, seg.Span.Begin) > 0 {
			return Segment{}, fmt.Errorf("segs out of order, %d end=%q %d begin=%q", i-1, prevSeg.Span.End, i, seg.Span.Begin)
		} else {
			firstSeg = seg
		}

		var root gotkv.Root
		if seg.Contents.Ref.IsZero() {
			r, err := mach.gotkv.NewEmpty(ctx, ss[1])
			if err != nil {
				return Segment{}, err
			}
			root = r
		} else {
			root = seg.Contents
		}
		if err := b.copyFrom(ctx, root, seg.Span); err != nil {
			return Segment{}, err
		}
		i++
		prevSeg = seg
	}
	out, err := b.Finish()
	if err != nil {
		return Segment{}, err
	}
	return Segment{
		Span: gotkv.Span{
			Begin: firstSeg.Span.Begin,
			End:   prevSeg.Span.End,
		},
		Contents: out.ToGotKV(),
	}, nil
}

// Promote promotes a segment to a Root if the segment has the correct first key.
func Promote(ctx context.Context, seg Segment) (*Root, error) {
	var key Key
	if err := unmarshalInfoKey(seg.Contents.First, &key); err != nil {
		panic(err)
	}
	if key.Path() != "" {
		return nil, fmt.Errorf("segment is not a valid gotfs.Root")
	}
	return &Root{
		Ref:   seg.Contents.Ref,
		Depth: seg.Contents.Depth,
	}, nil
}

// Splice is equivalent to Concat followed by Promote
func (mach *Machine) Splice(ctx context.Context, ss [2]stores.RW, segs []Segment) (*Root, error) {
	seg, err := mach.Concat(ctx, ss, slices.Values(segs))
	if err != nil {
		return nil, err
	}
	return Promote(ctx, seg)
}

func (mach *Machine) Exists(ctx context.Context, ms stores.Reading, root Root, p string) (bool, error) {
	_, err := mach.GetInfo(ctx, ms, root, p)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ExistsDir returns true if there is an INFO object at p which is a directory.
func (mach *Machine) ExistsDir(ctx context.Context, ms stores.Reading, root Root, p string) (bool, error) {
	info, err := mach.GetInfo(ctx, ms, root, p)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return info.Mode.IsDir(), nil
}
