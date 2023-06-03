package gotlob

import (
	"context"
	"io"

	"github.com/brendoncarroll/go-exp/streams"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/chunking"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/gotvc/got/pkg/metrics"
	"github.com/gotvc/got/pkg/units"
)

type Option func(o *Operator)

// WithFilter sets a filter function, so that the operator ignores
// any keys where fn(key) is false.
func WithFilter(fn func([]byte) bool) Option {
	return func(o *Operator) {
		o.keyFilter = fn
	}
}

// WithChunking sets the chunking strategy used by the Operator
func WithChunking(flushBetween bool, fn func(onChunk chunking.ChunkHandler) *chunking.ContentDefined) Option {
	return func(o *Operator) {
		o.newChunker = fn
		o.flushBetween = flushBetween
	}
}

type Operator struct {
	gotkv *gotkv.Operator
	gdat  *gdat.Operator

	newChunker   func(chunking.ChunkHandler) *chunking.ContentDefined
	keyFilter    func([]byte) bool
	flushBetween bool
}

func NewOperator(gkvop *gotkv.Operator, dop *gdat.Operator, opts ...Option) Operator {
	o := Operator{
		gotkv: gkvop,
		gdat:  dop,

		newChunker: func(onChunk chunking.ChunkHandler) *chunking.ContentDefined {
			return chunking.NewContentDefined(64, 1<<20, 1<<21, new([32]byte), onChunk)
		},
		keyFilter:    func([]byte) bool { return true },
		flushBetween: false,
	}
	for _, opt := range opts {
		opt(&o)
	}
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

func (o *Operator) SizeOf(ctx context.Context, ms cadata.Store, root Root, prefix []byte) (uint64, error) {
	key, _, err := o.MaxExtent(ctx, ms, root, gotkv.PrefixSpan(prefix))
	if err != nil {
		return 0, err
	}
	_, offset, err := ParseExtentKey(key)
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

func (op *Operator) post(ctx context.Context, s cadata.Store, data []byte) (*Extent, error) {
	ref, err := op.gdat.Post(ctx, s, data)
	if err != nil {
		return nil, err
	}
	return &Extent{Offset: 0, Length: uint32(len(data)), Ref: *ref}, nil
}

func (op *Operator) getExtentF(ctx context.Context, ds cadata.Store, ext *Extent, fn func([]byte) error) error {
	return op.gdat.GetF(ctx, ds, ext.Ref, func(data []byte) error {
		if err := checkExtentBounds(ext, len(data)); err != nil {
			return err
		}
		return fn(data[ext.Offset : ext.Offset+ext.Length])
	})
}

func (op *Operator) readExtent(ctx context.Context, buf []byte, ds cadata.Store, ext *Extent) (int, error) {
	n, err := op.gdat.Read(ctx, ds, ext.Ref, buf)
	if err != nil {
		return 0, err
	}
	if err := checkExtentBounds(ext, n); err != nil {
		return 0, err
	}
	return copy(buf[:], buf[ext.Offset:ext.Offset+ext.Length]), nil
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
			if streams.IsEOS(err) {
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
