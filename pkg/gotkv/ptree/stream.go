package ptree

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/streams"
	"github.com/dchest/siphash"
	"github.com/gotvc/got/pkg/maybe"
)

// NextIndexFromSlice returns a NextIndex function for StreamReaders which will
// iterate over the provided slice.
func NextIndexFromSlice[T, Ref any](idxs []Index[T, Ref]) func(ctx context.Context, dst *Index[T, Ref]) error {
	var i int
	return func(_ context.Context, dst *Index[T, Ref]) error {
		if i >= len(idxs) {
			return streams.EOS()
		}
		*dst = idxs[i]
		i++
		return nil
	}
}

type StreamReader[T, Ref any] struct {
	p StreamReaderParams[T, Ref]

	buf    []byte
	offset int
	n      int
}

type StreamReaderParams[T, Ref any] struct {
	Store     Getter[Ref]
	Decoder   Decoder[T, Ref]
	Compare   CompareFunc[T]
	NextIndex func(ctx context.Context, dst *Index[T, Ref]) error
}

func NewStreamReader[T, Ref any](params StreamReaderParams[T, Ref]) *StreamReader[T, Ref] {
	if params.Store == nil {
		panic("NewStreamReader nil Store")
	}
	if params.Decoder == nil {
		panic("NewStreamReader nil Decoder")
	}
	if params.NextIndex == nil {
		panic("NewStreamReader nil NextIndex")
	}
	if params.Compare == nil {
		panic("NewStreamReader nil Compare")
	}
	return &StreamReader[T, Ref]{
		p: params,

		buf: make([]byte, params.Store.MaxSize()),
	}
}

func (r *StreamReader[T, Ref]) Next(ctx context.Context, dst *T) error {
	if r.offset >= r.n {
		if err := r.loadNextBlob(ctx); err != nil {
			return err
		}
	}
	n, err := r.p.Decoder.Read(r.buf[r.offset:r.n], dst)
	if err != nil {
		return err
	}
	r.offset += n
	return nil
}

func (r *StreamReader[T, Ref]) Peek(ctx context.Context, dst *T) error {
	if r.offset >= r.n {
		if err := r.loadNextBlob(ctx); err != nil {
			return err
		}
	}
	return r.p.Decoder.Peek(r.buf[r.offset:r.n], dst)
}

func (r *StreamReader[T, Ref]) PeekNoLoad(dst *T) error {
	if r.offset >= r.n {
		return streams.EOS()
	}
	return r.p.Decoder.Peek(r.buf[r.offset:r.n], dst)
}

func (r *StreamReader[T, Ref]) Seek(ctx context.Context, gteq T) error {
	var x T
	for {
		if err := r.Peek(ctx, &x); err != nil {
			if streams.IsEOS(err) {
				return nil
			}
			return err
		}
		if r.p.Compare(x, gteq) >= 0 {
			return nil
		}
		if err := r.Next(ctx, &x); err != nil {
			return err
		}
	}
}

// Buffered returns the number of bytes in the StreamReader which have not been read.
func (r *StreamReader[T, Ref]) Buffered() int {
	return r.n - r.offset
}

func (r *StreamReader[T, Ref]) loadNextBlob(ctx context.Context) error {
	var idx Index[T, Ref]
	if err := r.p.NextIndex(ctx, &idx); err != nil {
		return err
	}
	n, err := r.p.Store.Get(ctx, idx.Ref, r.buf)
	if err != nil {
		return err
	}
	if n == 0 {
		return streams.EOS()
	}
	r.n = n
	r.offset = 0
	r.p.Decoder.Reset(idx)
	return nil
}

type IndexHandler[T, Ref any] func(Index[T, Ref]) error

type StreamWriter[T, Ref any] struct {
	p StreamWriterParams[T, Ref]

	buf []byte
	n   int

	first T
	prev  maybe.Maybe[T]
	count uint
}

type StreamWriterParams[T, Ref any] struct {
	Store    Poster[Ref]
	Seed     *[16]byte
	MeanSize int
	MaxSize  int
	Compare  func(a, b T) int
	Encoder  Encoder[T]
	// OnIndex must not retain the index after the call has ended.
	OnIndex IndexHandler[T, Ref]
	Copy    func(dst *T, src T)
}

func NewStreamWriter[T, Ref any](params StreamWriterParams[T, Ref]) *StreamWriter[T, Ref] {
	if params.Seed == nil {
		params.Seed = new([16]byte)
	}
	if params.Copy == nil {
		params.Copy = func(dst *T, src T) { *dst = src }
	}
	w := &StreamWriter[T, Ref]{
		p: params,

		buf: make([]byte, params.MaxSize),
	}
	w.p.Encoder.Reset()
	return w
}

func (w *StreamWriter[T, Ref]) Append(ctx context.Context, ent T) error {
	if w.prev.Ok && w.p.Compare(ent, w.prev.X) <= 0 {
		panic(fmt.Sprintf("out of order key: prev=%v current=%v", w.prev.X, ent))
	}
	entryLen := w.p.Encoder.EncodedLen(ent)
	if entryLen > w.p.MaxSize {
		return fmt.Errorf("entry (size=%d) exceeds maximum size %d", entryLen, w.p.MaxSize)
	}
	if entryLen+w.n > w.p.MaxSize {
		if err := w.flush(ctx, false); err != nil {
			return err
		}
	}

	offset := w.n
	n, err := w.p.Encoder.Write(w.buf[offset:], ent)
	if err != nil {
		return err
	}
	if n != entryLen {
		return fmt.Errorf("encoder reported inaccurate entry length, claimed=%d, actual=%d", entryLen, n)
	}
	if w.count == 0 {
		w.p.Copy(&w.first, ent)
	}
	w.n += n

	w.p.Copy(&w.prev.X, ent)
	w.prev.Ok = true

	w.count++
	// split after writing the entry
	if w.isSplitPoint(w.buf[offset : offset+n]) {
		if err := w.flush(ctx, true); err != nil {
			return err
		}
	}
	return nil
}

func (w *StreamWriter[T, Ref]) Buffered() int {
	return w.n
}

func (w *StreamWriter[T, Ref]) Flush(ctx context.Context) error {
	return w.flush(ctx, false)
}

func (w *StreamWriter[T, Ref]) flush(ctx context.Context, isNatural bool) error {
	if w.Buffered() == 0 {
		return nil
	}
	ref, err := w.p.Store.Post(ctx, w.buf[:w.n])
	if err != nil {
		return err
	}
	span := state.TotalSpan[T]()
	span = span.WithLowerIncl(w.first)
	span = span.WithUpperIncl(w.prev.X)
	if err := w.p.OnIndex(Index[T, Ref]{
		Ref:       ref,
		Span:      span,
		IsNatural: isNatural,
		Count:     w.count,
	}); err != nil {
		return err
	}
	var zero T
	w.p.Copy(&w.first, zero)
	w.p.Copy(&w.prev.X, zero)
	w.prev.Ok = false

	w.n = 0
	w.p.Encoder.Reset()
	w.count = 0
	return nil
}

func (w *StreamWriter[T, Ref]) isSplitPoint(data []byte) bool {
	r := sum64(data, w.p.Seed)
	prob := math.MaxUint64 / uint64(w.p.MeanSize) * uint64(len(data))
	return r < prob
}

func min(a, b int) int {
	if b < a {
		return b
	}
	return a
}

func sum64(data []byte, key *[16]byte) uint64 {
	en := binary.LittleEndian
	k1 := en.Uint64(key[:8])
	k2 := en.Uint64(key[8:])
	return siphash.Hash(k1, k2, data)
}
