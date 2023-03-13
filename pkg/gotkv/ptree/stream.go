package ptree

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/dchest/siphash"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
)

type Index[T, Ref any] struct {
	Ref   Ref
	First T
}

type blobReader[T, Ref any] struct {
	dec     Decoder[T, Ref]
	compare func(a, b T) int

	buf         []byte
	offset, len int
}

// newBlobReader reads the entries from data, using firstKey as the first key
// firstKey and data must not be modified while using the blobReader
func newBlobReader[T, Ref any](dec Decoder[T, Ref], parent Index[T, Ref], data []byte) *blobReader[T, Ref] {
	dec.Reset(parent)
	return &blobReader[T, Ref]{
		dec: dec,
		buf: data,
		len: len(data), // TODO: eventually we want to reuse the buffer
	}
}

func (r *blobReader[T, Ref]) SeekIndexes(ctx context.Context, gteq T) error {
	var ent T
	for {
		// if the prevKey is already <= gteq, then don't bother with this
		//if r.compare(r.prevKey, gteq) <= 0 {
		//	return nil
		//}
		// check to see if the next key is also <= gteq
		if err := r.Peek(ctx, &ent); err != nil {
			return err
		}
		if r.compare(ent, gteq) >= 0 {
			return nil
		}
		if err := r.Next(ctx, &ent); err != nil {
			return err
		}
	}
}

func (r *blobReader[T, Ref]) Seek(ctx context.Context, gteq T) error {
	var ent T
	for {
		if err := r.Peek(ctx, &ent); err != nil {
			if err == kvstreams.EOS {
				return nil
			}
			return err
		}
		if r.compare(ent, gteq) >= 0 {
			return nil
		}
		if err := r.Next(ctx, &ent); err != nil {
			return err
		}
	}
}

func (r *blobReader[T, Ref]) Next(ctx context.Context, ent *T) error {
	if err := r.next(ctx, ent); err != nil {
		return err
	}
	return nil
}

func (r *blobReader[T, Ref]) Peek(ctx context.Context, ent *T) error {
	return r.dec.PeekEntry(r.buf[r.offset:r.len], ent)
}

// next reads the next entry, but does not update r.prevKey
func (r *blobReader[T, Ref]) next(ctx context.Context, ent *T) error {
	if r.len-r.offset == 0 {
		return EOS
	}
	n, err := r.dec.ReadEntry(r.buf[r.offset:r.len], ent)
	if err != nil {
		return err
	}
	r.offset += n
	return nil
}

type StreamReader[T, Ref any] struct {
	s   Getter[Ref]
	cmp CompareFunc[T]
	dec Decoder[T, Ref]

	idxs      []Index[T, Ref]
	nextIndex int
	br        *blobReader[T, Ref]
}

type StreamReaderParams[T, Ref any] struct {
	Store   Getter[Ref]
	Decoder Decoder[T, Ref]
	Compare CompareFunc[T]
	Indexes []Index[T, Ref]
}

func NewStreamReader[T, Ref any](params StreamReaderParams[T, Ref]) *StreamReader[T, Ref] {
	if params.Store == nil {
		panic("NewStreamReader nil Store")
	}
	if params.Decoder == nil {
		panic("NewStreamReader nil Decoder")
	}
	idxs := params.Indexes
	for i := 0; i < len(idxs)-1; i++ {
		if params.Compare(idxs[i].First, idxs[i+1].First) >= 0 {
			panic(fmt.Sprintf("StreamReader: unordered indexes %q >= %q", idxs[i].First, idxs[i+1].First))
		}
	}
	return &StreamReader[Ref]{
		p: params,

		idxs: idxs,
	}
}

func (r *StreamReader[T, Ref]) Next(ctx context.Context, ent *T) error {
	return r.withBlobReader(ctx, func(br *blobReader[T, Ref]) error {
		return br.Next(ctx, ent)
	})
}

func (r *StreamReader[T, Ref]) Peek(ctx context.Context, ent *T) error {
	return r.withBlobReader(ctx, func(br *blobReader[T, Ref]) error {
		return br.Peek(ctx, ent)
	})
}

func (r *StreamReader[T, Ref]) SeekIndexes(ctx context.Context, gteq T) error {
	if err := r.seekCommon(ctx, gteq); err != nil {
		return err
	}
	if r.br == nil {
		return nil
	}
	return r.br.SeekIndexes(ctx, gteq)
}

func (r *StreamReader[T, Ref]) Seek(ctx context.Context, gteq T) error {
	if err := r.seekCommon(ctx, gteq); err != nil {
		return err
	}
	if r.br == nil {
		return nil
	}
	return r.br.Seek(ctx, gteq)
}

func (r *StreamReader[T, Ref]) seekCommon(ctx context.Context, gteq T) error {
	if len(r.idxs) < 1 {
		return nil
	}
	var targetIndex int
	for i := 1; i < len(r.idxs); i++ {
		if r.cmp(r.idxs[i].First, gteq) <= 0 {
			targetIndex = i
		} else {
			break
		}
	}
	if r.br == nil || r.nextIndex != targetIndex+1 {
		var err error
		r.br, err = r.getBlobReader(ctx, r.idxs[targetIndex])
		if err != nil {
			return err
		}
		r.nextIndex = targetIndex + 1
	}
	return nil
}

func (r *StreamReader[T, Ref]) withBlobReader(ctx context.Context, fn func(*blobReader[T, Ref]) error) error {
	if r.br == nil {
		if r.nextIndex == len(r.idxs) {
			return kvstreams.EOS
		}
		idx := r.idxs[r.nextIndex]
		r.nextIndex++
		var err error
		r.br, err = r.getBlobReader(ctx, idx)
		if err != nil {
			return err
		}
	}
	err := fn(r.br)
	if err == kvstreams.EOS {
		r.br = nil
		return r.withBlobReader(ctx, fn)
	}
	return err
}

func (r *StreamReader[T, Ref]) getBlobReader(ctx context.Context, idx Index[T, Ref]) (*blobReader[T, Ref], error) {
	buf := make([]byte, r.p.Store.MaxSize())
	n, err := r.p.Store.Get(ctx, idx.Ref, buf)
	if err != nil {
		return nil, err
	}
	return newBlobReader(r.p.Decoder, idx.First, buf[:n]), nil
}

type IndexHandler[T, Ref any] func(Index[T, Ref]) error

type StreamWriter[T, Ref any] struct {
	p StreamWriterParams[T, Ref]

	buf []byte
	n   int

	first      T
	prev       T
	prevExists bool
}

type StreamWriterParams[T, Ref any] struct {
	Store    Poster[Ref]
	Seed     *[16]byte
	MeanSize int
	MaxSize  int
	Compare  func(a, b T) int
	Encoder  Encoder[T]
	OnIndex  IndexHandler[T, Ref]
	Copy     func(dst *T, src T)
}

func NewStreamWriter[T, Ref any](params StreamWriterParams[T, Ref]) *StreamWriter[T, Ref] {
	if params.Seed == nil {
		params.Seed = new([16]byte)
	}
	w := &StreamWriter[T, Ref]{
		p: params,

		buf: make([]byte, params.MaxSize),
	}
	w.p.Encoder.Reset()
	return w
}

func (w *StreamWriter[T, Ref]) Append(ctx context.Context, ent T) error {
	if w.prevExists && w.p.Compare(ent, w.prev) <= 0 {
		panic(fmt.Sprintf("out of order key: prev=%v current=%v", w.prev, ent))
	}
	entryLen := w.p.Encoder.EncodedLen(ent)
	if entryLen > w.p.MaxSize {
		return fmt.Errorf("entry (size=%d) exceeds maximum size %d", entryLen, w.p.MaxSize)
	}
	if entryLen+w.n > w.p.MaxSize {
		if err := w.Flush(ctx); err != nil {
			return err
		}
	}

	offset := w.n
	n, err := w.p.Encoder.WriteEntry(w.buf[offset:], ent)
	if err != nil {
		return err
	}
	if n != entryLen {
		return fmt.Errorf("encoder reported inaccurate entry length, claimed=%d, actual=%d", entryLen, n)
	}
	if offset == 0 {
		w.p.Copy(&w.first, ent)
	}
	w.n += n

	w.p.Copy(&w.prev, ent)
	w.prevExists = true
	// split after writing the entry
	if w.isSplitPoint(w.buf[offset : offset+n]) {
		if err := w.Flush(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (w *StreamWriter[T, Ref]) Buffered() int {
	return w.n
}

func (w *StreamWriter[T, Ref]) Flush(ctx context.Context) error {
	if w.Buffered() == 0 {
		return nil
	}
	ref, err := w.p.Store.Post(ctx, w.buf[:w.n])
	if err != nil {
		return err
	}
	var first T
	w.p.Copy(&first, w.first)
	if err := w.p.OnIndex(Index[T, Ref]{
		First: first,
		Ref:   ref,
	}); err != nil {
		return err
	}
	var zero T
	w.p.Copy(&w.first, zero)
	w.n = 0
	w.p.Encoder.Reset()
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
