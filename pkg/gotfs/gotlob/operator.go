package gotlob

import (
	"context"
	"errors"
	"io"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/chunking"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/gotvc/got/pkg/metrics"
	"github.com/gotvc/got/pkg/units"
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

func WithFilter(fn func([]byte) bool) Option {
	return func(o *Operator) {
		o.keyFilter = fn
	}
}

type Operator struct {
	maxBlobSize                                 int
	minSizeData, averageSizeData, averageSizeKV int
	salt                                        *[32]byte
	rawCacheSize, metaCacheSize                 int
	keyFilter                                   func([]byte) bool
	flushBetween                                bool

	rawOp        gdat.Operator
	gotkv        gotkv.Operator
	chunkingSeed *[32]byte
}

func NewOperator(opts ...Option) Operator {
	o := Operator{
		maxBlobSize:     DefaultMaxBlobSize,
		minSizeData:     DefaultMinBlobSizeData,
		averageSizeData: DefaultAverageBlobSizeData,
		averageSizeKV:   DefaultAverageBlobSizeKV,

		salt:          &[32]byte{},
		rawCacheSize:  8,
		metaCacheSize: 16,
		keyFilter:     func([]byte) bool { return true },
		flushBetween:  false,
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
		o.averageSizeKV,
		o.maxBlobSize,
		gotkv.WithDataOperator(metaOp),
		gotkv.WithSeed(&treeSeed),
	)
	return o
}

func (o *Operator) CreateExtents(ctx context.Context, ds cadata.Store, r io.Reader) ([]*Extent, error) {
	var exts []*Extent
	chunker := o.newChunker(func(data []byte) error {
		ext, err := o.post(ctx, ds, data)
		if err != nil {
			return err
		}
		metrics.AddInt(ctx, "data_in", len(data), units.Bytes)
		metrics.AddInt(ctx, "blobs_in", 1, "blobs")
		exts = append(exts, ext)
		return nil
	})
	if _, err := io.Copy(chunker, r); err != nil {
		return nil, err
	}
	if err := chunker.Flush(); err != nil {
		return nil, err
	}
	return exts, nil
}

func (o *Operator) SizeOf(ctx context.Context, ms cadata.Store, root Root, key []byte) (uint64, error) {
	ent, err := o.gotkv.MaxEntry(ctx, ms, root, gotkv.PrefixSpan(key))
	if err != nil {
		return 0, err
	}
	_, offset, err := ParseExtentKey(ent.Key)
	if err != nil {
		return 0, err
	}
	return offset, nil
}

func (o *Operator) Splice(ctx context.Context, ms, ds cadata.Store, segs []Segment) (*Root, error) {
	b := o.NewBuilder(ctx, ms, ds)
	for _, seg := range segs {
		if err := b.CopyFrom(ctx, seg.Root, seg.Span); err != nil {
			return nil, err
		}
	}
	return b.Finish(ctx)
}

func (op *Operator) GotKV() *gotkv.Operator {
	return &op.gotkv
}

func (op *Operator) newChunker(fn chunking.ChunkHandler) *chunking.ContentDefined {
	return chunking.NewContentDefined(op.minSizeData, op.averageSizeData, op.maxBlobSize, op.chunkingSeed, fn)
}

func (op *Operator) post(ctx context.Context, s cadata.Store, data []byte) (*Extent, error) {
	l := len(data)
	for len(data)%64 != 0 {
		data = append(data, 0x00)
	}
	ref, err := op.rawOp.Post(ctx, s, data)
	if err != nil {
		return nil, err
	}
	return &Extent{Offset: 0, Length: uint32(l), Ref: *ref}, nil
}

func (op *Operator) readExtent(ctx context.Context, buf []byte, ds cadata.Store, ext *Extent) (int, error) {
	n, err := op.rawOp.Read(ctx, ds, ext.Ref, buf)
	if err != nil {
		return 0, err
	}
	if err := checkExtentBounds(ext, n); err != nil {
		return 0, err
	}
	return copy(buf[:], buf[ext.Offset:ext.Offset+ext.Length]), nil
}

func (op *Operator) getExtentF(ctx context.Context, ds cadata.Store, ext *Extent, fn func([]byte) error) error {
	return op.rawOp.GetF(ctx, ds, ext.Ref, func(data []byte) error {
		if err := checkExtentBounds(ext, len(data)); err != nil {
			return err
		}
		return fn(data[ext.Offset : ext.Offset+ext.Length])
	})
}

// maxEntry finds the maximum extent entry in root within span.
func (op *Operator) MaxExtent(ctx context.Context, ms cadata.Store, root Root, span Span) ([]byte, *Extent, error) {
	for {
		ent, err := op.gotkv.MaxEntry(ctx, ms, root, span)
		if err != nil {
			return nil, nil, err
		}
		if ent == nil {
			return nil, nil, nil
		}
		if op.keyFilter(ent.Key) {
			ext, err := ParseExtent(ent.Value)
			if err != nil {
				return nil, nil, err
			}
			return ent.Key, ext, nil
		}
		span.End = ent.Key
	}
}

func (op *Operator) MinExtent(ctx context.Context, ms cadata.Store, root Root, span Span) ([]byte, *Extent, error) {
	it := op.gotkv.NewIterator(ms, root, span)
	var ent gotkv.Entry
	for {
		if err := it.Next(ctx, &ent); err != nil {
			if errors.Is(err, gotkv.EOS) {
				return nil, nil, nil
			}
			return nil, nil, err
		}
		if op.keyFilter(ent.Key) {
			ext, err := ParseExtent(ent.Value)
			if err != nil {
				return nil, nil, err
			}
			return ent.Key, ext, nil
		}
	}
}
