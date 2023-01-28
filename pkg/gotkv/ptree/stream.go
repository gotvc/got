package ptree

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/dchest/siphash"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
)

type Index[Ref any] struct {
	Ref   Ref
	First []byte
}

func (idx Index[Ref]) Clone() Index[Ref] {
	return Index[Ref]{
		Ref:   idx.Ref,
		First: append([]byte{}, idx.First...),
	}
}

type blobReader struct {
	dec     Decoder
	compare func(a, b []byte) int

	buf         []byte
	offset, len int

	prevKey []byte
}

// newBlobReader reads the entries from data, using firstKey as the first key
// firstKey and data must not be modified while using the blobReader
func newBlobReader(dec Decoder, firstKey []byte, data []byte) *blobReader {
	dec.Reset(firstKey)
	return &blobReader{
		dec: dec,
		buf: data,
		len: len(data), // TODO: eventually we want to reuse the buffer
	}
}

func (r *blobReader) SeekIndexes(ctx context.Context, gteq []byte) error {
	var ent Entry
	for {
		// if the prevKey is already <= gteq, then don't bother with this
		if r.compare(r.prevKey, gteq) <= 0 {
			return nil
		}
		// check to see if the next key is also <= gteq
		if err := r.Peek(ctx, &ent); err != nil {
			return err
		}
		if r.compare(ent.Key, gteq) >= 0 {
			return nil
		}
		if err := r.Next(ctx, &ent); err != nil {
			return err
		}
	}
}

func (r *blobReader) Seek(ctx context.Context, gteq []byte) error {
	var ent Entry
	for {
		if err := r.Peek(ctx, &ent); err != nil {
			if err == kvstreams.EOS {
				return nil
			}
			return err
		}
		if r.compare(ent.Key, gteq) >= 0 {
			return nil
		}
		if err := r.Next(ctx, &ent); err != nil {
			return err
		}
	}
}

func (r *blobReader) Next(ctx context.Context, ent *Entry) error {
	if err := r.next(ctx, ent); err != nil {
		return err
	}
	r.setPrevKey(ent.Key)
	return nil
}

func (r *blobReader) Peek(ctx context.Context, ent *Entry) error {
	return r.dec.PeekEntry(r.buf[r.offset:r.len], ent)
}

// next reads the next entry, but does not update r.prevKey
func (r *blobReader) next(ctx context.Context, ent *Entry) error {
	if r.len-r.offset == 0 {
		return kvstreams.EOS
	}
	n, err := r.dec.ReadEntry(r.buf[r.offset:r.len], ent)
	if err != nil {
		return err
	}
	r.offset += n
	return nil
}

func (r *blobReader) setPrevKey(x []byte) {
	r.prevKey = append(r.prevKey[:0], x...)
}

type StreamReader[Ref any] struct {
	s   Getter[Ref]
	cmp CompareFunc
	dec Decoder

	idxs      []Index[Ref]
	nextIndex int
	br        *blobReader
}

type StreamReaderParams[Ref any] struct {
	Store   Getter[Ref]
	Decoder Decoder
	Compare CompareFunc
	Indexes []Index[Ref]
}

func NewStreamReader[Ref any](params StreamReaderParams[Ref]) *StreamReader[Ref] {
	if params.Store == nil {
		panic("NewStreamReader nil Store")
	}
	if params.Decoder == nil {
		panic("NewStreamReader nil Decoder")
	}
	idxs := params.Indexes
	for i := 0; i < len(idxs)-1; i++ {
		if bytes.Compare(idxs[i].First, idxs[i+1].First) >= 0 {
			panic(fmt.Sprintf("StreamReader: unordered indexes %q >= %q", idxs[i].First, idxs[i+1].First))
		}
	}
	return &StreamReader[Ref]{
		s:   params.Store,
		cmp: params.Compare,
		dec: params.Decoder,

		idxs: idxs,
	}
}

func (r *StreamReader[Ref]) Next(ctx context.Context, ent *Entry) error {
	return r.withBlobReader(ctx, func(br *blobReader) error {
		return br.Next(ctx, ent)
	})
}

func (r *StreamReader[Ref]) Peek(ctx context.Context, ent *Entry) error {
	return r.withBlobReader(ctx, func(br *blobReader) error {
		return br.Peek(ctx, ent)
	})
}

func (r *StreamReader[Ref]) SeekIndexes(ctx context.Context, gteq []byte) error {
	if err := r.seekCommon(ctx, gteq); err != nil {
		return err
	}
	if r.br == nil {
		return nil
	}
	return r.br.SeekIndexes(ctx, gteq)
}

func (r *StreamReader[Ref]) Seek(ctx context.Context, gteq []byte) error {
	if err := r.seekCommon(ctx, gteq); err != nil {
		return err
	}
	if r.br == nil {
		return nil
	}
	return r.br.Seek(ctx, gteq)
}

func (r *StreamReader[Ref]) seekCommon(ctx context.Context, gteq []byte) error {
	if len(r.idxs) < 1 {
		return nil
	}
	var targetIndex int
	for i := 1; i < len(r.idxs); i++ {
		if bytes.Compare(r.idxs[i].First, gteq) <= 0 {
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

func (r *StreamReader[Ref]) withBlobReader(ctx context.Context, fn func(*blobReader) error) error {
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

func (r *StreamReader[Ref]) getBlobReader(ctx context.Context, idx Index[Ref]) (*blobReader, error) {
	buf := make([]byte, r.s.MaxSize())
	n, err := r.s.Get(ctx, idx.Ref, buf)
	if err != nil {
		return nil, err
	}
	return newBlobReader(r.dec, idx.First, buf[:n]), nil
}

type IndexHandler[Ref any] func(Index[Ref]) error

type StreamWriter[Ref any] struct {
	s                 Poster[Ref]
	enc               Encoder
	compare           func(a, b []byte) int
	onIndex           IndexHandler[Ref]
	seed              *[16]byte
	meanSize, maxSize int

	buf []byte
	n   int

	firstKey []byte
	prevKey  []byte
}

type StreamWriterParams[Ref any] struct {
	Store    Poster[Ref]
	Seed     *[16]byte
	MeanSize int
	MaxSize  int
	Compare  func(a, b []byte) int
	Encoder  Encoder
	OnIndex  IndexHandler[Ref]
}

func NewStreamWriter[Ref any](params StreamWriterParams[Ref]) *StreamWriter[Ref] {
	if params.Seed == nil {
		params.Seed = new([16]byte)
	}
	w := &StreamWriter[Ref]{
		s:        params.Store,
		compare:  bytes.Compare,
		enc:      params.Encoder,
		onIndex:  params.OnIndex,
		seed:     params.Seed,
		meanSize: params.MeanSize,
		maxSize:  params.MaxSize,

		buf: make([]byte, params.MaxSize),
	}
	w.enc.Reset()
	return w
}

func (w *StreamWriter[Ref]) Append(ctx context.Context, ent Entry) error {
	if w.prevKey != nil && w.compare(ent.Key, w.prevKey) <= 0 {
		panic(fmt.Sprintf("out of order key: prev=%q key=%q", w.prevKey, ent.Key))
	}
	entryLen := w.enc.EncodedLen(ent)
	if entryLen > w.maxSize {
		return fmt.Errorf("entry (size=%d) exceeds maximum size %d", entryLen, w.maxSize)
	}
	if entryLen+w.n > w.maxSize {
		if err := w.Flush(ctx); err != nil {
			return err
		}
	}

	offset := w.n
	n, err := w.enc.WriteEntry(w.buf[offset:], ent)
	if err != nil {
		return err
	}
	if n != entryLen {
		return fmt.Errorf("encoder reported inaccurate entry length, claimed=%d, actual=%d", entryLen, n)
	}
	if offset == 0 {
		w.firstKey = append(w.firstKey[:0], ent.Key...)
	}
	w.n += n

	w.prevKey = append(w.prevKey[:0], ent.Key...)
	// split after writing the entry
	if w.isSplitPoint(w.buf[offset : offset+n]) {
		if err := w.Flush(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (w *StreamWriter[Ref]) Buffered() int {
	return w.n
}

func (w *StreamWriter[Ref]) Flush(ctx context.Context) error {
	if w.Buffered() == 0 {
		if len(w.firstKey) != 0 {
			panic("StreamWriter: firstKey set for empty buffer")
		}
		return nil
	}
	ref, err := w.s.Post(ctx, w.buf[:w.n])
	if err != nil {
		return err
	}
	if err := w.onIndex(Index[Ref]{
		First: w.firstKey,
		Ref:   ref,
	}); err != nil {
		return err
	}
	w.firstKey = w.firstKey[:0]
	w.n = 0
	w.enc.Reset()
	return nil
}

func (w *StreamWriter[Ref]) isSplitPoint(data []byte) bool {
	r := sum64(data, w.seed)
	prob := math.MaxUint64 / uint64(w.meanSize) * uint64(len(data))
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
