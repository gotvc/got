package gotlob

import (
	"context"
	"io"

	"github.com/gotvc/got/src/chunking"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/metrics"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/units"
	"go.brendoncarroll.net/exp/streams"
)

type Option func(a *Machine)

// WithFilter sets a filter function, so that the operator ignores
// any keys where fn(key) is false.
func WithFilter(fn func([]byte) bool) Option {
	return func(a *Machine) {
		a.keyFilter = fn
	}
}

// WithChunking sets the chunking strategy used by the Machine
func WithChunking(flushBetween bool, fn func(onChunk chunking.ChunkHandler) *chunking.ContentDefined) Option {
	return func(a *Machine) {
		a.newChunker = fn
		a.flushBetween = flushBetween
	}
}

type Machine struct {
	gotkv *gotkv.Machine
	gdat  *gdat.Machine

	newChunker   func(chunking.ChunkHandler) *chunking.ContentDefined
	keyFilter    func([]byte) bool
	flushBetween bool
}

func NewMachine(gkvop *gotkv.Machine, dop *gdat.Machine, opts ...Option) Machine {
	o := Machine{
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

func (a *Machine) CreateExtents(ctx context.Context, ds stores.RW, r io.Reader) ([]*Extent, error) {
	var exts []*Extent
	chunker := a.newChunker(func(data []byte) error {
		ext, err := a.post(ctx, ds, data)
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

func (a *Machine) SizeOf(ctx context.Context, ms stores.Reading, root Root, prefix []byte) (uint64, error) {
	key, _, err := a.MaxExtent(ctx, ms, root, gotkv.PrefixSpan(prefix))
	if err != nil {
		return 0, err
	}
	_, offset, err := ParseExtentKey(key)
	if err != nil {
		return 0, err
	}
	return offset, nil
}

func (a *Machine) Splice(ctx context.Context, ss [2]stores.RW, segs []Segment) (*Root, error) {
	b := a.NewBuilder(ctx, ss[1], ss[0])
	for _, seg := range segs {
		if err := b.CopyFrom(ctx, seg.Root, seg.Span); err != nil {
			return nil, err
		}
	}
	return b.Finish(ctx)
}

func (ag *Machine) post(ctx context.Context, s stores.RW, data []byte) (*Extent, error) {
	ref, err := ag.gdat.Post(ctx, s, data)
	if err != nil {
		return nil, err
	}
	return &Extent{Offset: 0, Length: uint32(len(data)), Ref: *ref}, nil
}

func (ag *Machine) getExtentF(ctx context.Context, ds stores.Reading, ext *Extent, fn func([]byte) error) error {
	return ag.gdat.GetF(ctx, ds, ext.Ref, func(data []byte) error {
		if err := checkExtentBounds(ext, len(data)); err != nil {
			return err
		}
		return fn(data[ext.Offset : ext.Offset+ext.Length])
	})
}

func (ag *Machine) readExtent(ctx context.Context, buf []byte, ds stores.Reading, ext *Extent) (int, error) {
	n, err := ag.gdat.Read(ctx, ds, ext.Ref, buf)
	if err != nil {
		return 0, err
	}
	if err := checkExtentBounds(ext, n); err != nil {
		return 0, err
	}
	return copy(buf[:], buf[ext.Offset:ext.Offset+ext.Length]), nil
}

// maxEntry finds the maximum extent entry in root within span.
func (ag *Machine) MaxExtent(ctx context.Context, ms stores.Reading, root Root, span Span) ([]byte, *Extent, error) {
	for {
		ent, err := ag.gotkv.MaxEntry(ctx, ms, root, span)
		if err != nil {
			return nil, nil, err
		}
		if ent == nil {
			return nil, nil, nil
		}
		if ag.keyFilter(ent.Key) {
			ext, err := ParseExtent(ent.Value)
			if err != nil {
				return nil, nil, err
			}
			return ent.Key, ext, nil
		}
		span.End = ent.Key
	}
}

func (ag *Machine) MinExtent(ctx context.Context, ms stores.Reading, root Root, span Span) ([]byte, *Extent, error) {
	it := ag.gotkv.NewIterator(ms, root, span)
	var ent gotkv.Entry
	for {
		if err := streams.NextUnit(ctx, it, &ent); err != nil {
			if streams.IsEOS(err) {
				return nil, nil, nil
			}
			return nil, nil, err
		}
		if ag.keyFilter(ent.Key) {
			ext, err := ParseExtent(ent.Value)
			if err != nil {
				return nil, nil, err
			}
			return ent.Key, ext, nil
		}
	}
}
