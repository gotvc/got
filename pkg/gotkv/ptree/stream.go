package ptree

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math"

	"github.com/dchest/siphash"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
	"github.com/pkg/errors"
)

type Ref = gdat.Ref

type Index struct {
	Ref   Ref
	First []byte
}

type blobReader struct {
	compare  func(a, b []byte) int
	br       bytes.Reader
	firstKey []byte
	prevKey  []byte
}

// newBlobReader reads the entries from data, using firstKey as the first key
// firstKey and data must not be modified while using the blobReader
func newBlobReader(firstKey []byte, data []byte) *blobReader {
	return &blobReader{
		br:       *bytes.NewReader(data),
		firstKey: firstKey,
		prevKey:  append([]byte{}, firstKey...),
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
	l1 := r.br.Len()
	defer func() {
		l2 := r.br.Len()
		for i := 0; i < l1-l2; i++ {
			if err := r.br.UnreadByte(); err != nil {
				panic(err)
			}
		}
	}()
	return r.next(ctx, ent)
}

// next reads the next entry, but does not update r.prevKey
func (r *blobReader) next(ctx context.Context, ent *Entry) error {
	if r.br.Len() == 0 {
		return kvstreams.EOS
	}
	return readEntry(ent, &r.br, r.prevKey, r.br.Len())
}

func (r *blobReader) setPrevKey(x []byte) {
	r.prevKey = append(r.prevKey[:0], x...)
}

type StreamReader struct {
	s   Getter
	cmp CompareFunc

	idxs      []Index
	nextIndex int
	br        *blobReader
}

type StreamReaderParams struct {
	Store   Getter
	Compare CompareFunc
	Indexes []Index
}

func NewStreamReader(params StreamReaderParams) *StreamReader {
	idxs := params.Indexes
	for i := 0; i < len(idxs)-1; i++ {
		if bytes.Compare(idxs[i].First, idxs[i+1].First) >= 0 {
			panic(fmt.Sprintf("StreamReader: unordered indexes %q >= %q", idxs[i].First, idxs[i+1].First))
		}
	}
	return &StreamReader{
		s:    params.Store,
		cmp:  params.Compare,
		idxs: idxs,
	}
}

func (r *StreamReader) Next(ctx context.Context, ent *Entry) error {
	return r.withBlobReader(ctx, func(br *blobReader) error {
		return br.Next(ctx, ent)
	})
}

func (r *StreamReader) Peek(ctx context.Context, ent *Entry) error {
	return r.withBlobReader(ctx, func(br *blobReader) error {
		return br.Peek(ctx, ent)
	})
}

func (r *StreamReader) SeekIndexes(ctx context.Context, gteq []byte) error {
	if err := r.seekCommon(ctx, gteq); err != nil {
		return err
	}
	if r.br == nil {
		return nil
	}
	return r.br.SeekIndexes(ctx, gteq)
}

func (r *StreamReader) Seek(ctx context.Context, gteq []byte) error {
	if err := r.seekCommon(ctx, gteq); err != nil {
		return err
	}
	if r.br == nil {
		return nil
	}
	return r.br.Seek(ctx, gteq)
}

func (r *StreamReader) seekCommon(ctx context.Context, gteq []byte) error {
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

func (r *StreamReader) withBlobReader(ctx context.Context, fn func(*blobReader) error) error {
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

func (r *StreamReader) getBlobReader(ctx context.Context, idx Index) (*blobReader, error) {
	buf := make([]byte, r.s.MaxSize())
	n, err := r.s.Get(ctx, idx.Ref, buf)
	if err != nil {
		return nil, err
	}
	return newBlobReader(idx.First, buf[:n]), nil
}

type IndexHandler = func(Index) error

type StreamWriter struct {
	s                 Poster
	enc               Encoder
	compare           func(a, b []byte) int
	onIndex           IndexHandler
	seed              *[16]byte
	meanSize, maxSize int

	buf []byte
	n   int

	firstKey []byte
	prevKey  []byte
}

type StreamWriterParams struct {
	Store    Poster
	Seed     *[16]byte
	MeanSize int
	MaxSize  int
	Compare  func(a, b []byte) int
	Encoder  Encoder
	OnIndex  IndexHandler
}

func NewStreamWriter(params StreamWriterParams) *StreamWriter {
	if params.Seed == nil {
		params.Seed = new([16]byte)
	}
	w := &StreamWriter{
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

func (w *StreamWriter) Append(ctx context.Context, ent Entry) error {
	if w.prevKey != nil && w.compare(ent.Key, w.prevKey) <= 0 {
		panic(fmt.Sprintf("out of order key: prev=%q key=%q", w.prevKey, ent.Key))
	}
	entryLen := w.enc.EncodedLen(ent)
	if entryLen > w.maxSize {
		return errors.Errorf("entry (size=%d) exceeds maximum size %d", entryLen, w.maxSize)
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
	w.n = offset + n
	if offset == 0 {
		w.firstKey = append(w.firstKey[:0], ent.Key...)
	}
	w.prevKey = append(w.prevKey[:0], ent.Key...)
	// split after writing the entry
	if w.isSplitPoint(w.buf[offset : offset+n]) {
		if err := w.Flush(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (w *StreamWriter) Buffered() int {
	return w.n
}

func (w *StreamWriter) Flush(ctx context.Context) error {
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
	if err := w.onIndex(Index{
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

func (w *StreamWriter) isSplitPoint(data []byte) bool {
	r := sum64(data, w.seed)
	prob := math.MaxUint64 / uint64(w.meanSize) * uint64(len(data))
	return r < prob
}

// writeUvarint writes x varint-encoded to buf
func writeUvarint(w *bytes.Buffer, x uint64) error {
	lenBuf := [binary.MaxVarintLen64]byte{}
	n := binary.PutUvarint(lenBuf[:], uint64(x))
	_, err := w.Write(lenBuf[:n])
	return err
}

// writeLPBytes writes len(x) varint-encoded, followed by x, to buf
func writeLPBytes(w *bytes.Buffer, x []byte) error {
	if err := writeUvarint(w, uint64(len(x))); err != nil {
		return err
	}
	_, err := w.Write(x)
	return err
}

// readLPBytes reads a varint from br, and then appends that many bytes from br to out
// it returns the new slice, or an error.
func readLPBytes(out []byte, br *bytes.Reader, max int) ([]byte, error) {
	l, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, err
	}
	if l > uint64(max) {
		return nil, errors.Errorf("lp bytestring exceeds max size %d > %d", l, max)
	}
	for i := uint64(0); i < l; i++ {
		b, err := br.ReadByte()
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}

// readEntry reads an entry into out
func readEntry(out *Entry, br *bytes.Reader, prevKey []byte, maxSize int) error {
	totalLen, err := binary.ReadUvarint(br)
	if err != nil {
		return err
	}
	if totalLen > uint64(maxSize) {
		return errors.Errorf("entry exceeds max size: %d > %d", totalLen, maxSize)
	}
	l1 := br.Len()
	// key
	out.Key = out.Key[:0]
	keyBackspace, err := binary.ReadUvarint(br)
	if err != nil {
		return err
	}
	if int(keyBackspace) > len(prevKey) {
		return errors.Errorf("backspace is > len(prevKey): prevKey=%q bs=%d", prevKey, keyBackspace)
	}
	end := len(prevKey) - int(keyBackspace)
	out.Key = append(out.Key, prevKey[:end]...)
	out.Key, err = readLPBytes(out.Key, br, MaxKeySize)
	if err != nil {
		return err
	}
	// value
	out.Value = out.Value[:0]
	out.Value, err = readLPBytes(out.Value, br, maxSize)
	if err != nil {
		return err
	}
	// check we read the right amount
	l2 := br.Len()
	if uint64(l1-l2) != totalLen {
		log.Println(l1, l2, totalLen)
		return errors.Errorf("invalid entry")
	}
	return nil
}

func writeEntry(w *bytes.Buffer, prevKey []byte, ent Entry) {
	cpLen := commonPrefix(prevKey, ent.Key)
	keySuffix := ent.Key[cpLen:]
	backspace := uint32(len(prevKey) - cpLen)

	l := computeEntryLen(prevKey, ent)
	if err := writeUvarint(w, uint64(l)); err != nil {
		panic(err)
	}
	if err := writeUvarint(w, uint64(backspace)); err != nil {
		panic(err)
	}
	if err := writeLPBytes(w, keySuffix); err != nil {
		panic(err)
	}
	if err := writeLPBytes(w, ent.Value); err != nil {
		panic(err)
	}
}

func computeEntryLen(prevKey []byte, ent Entry) int {
	cpLen := commonPrefix(prevKey, ent.Key)
	keySuffix := ent.Key[cpLen:]
	backspace := uint32(len(prevKey) - cpLen)

	var total int
	total += uvarintLen(uint64(backspace))
	total += uvarintLen(uint64(len(keySuffix)))
	total += len(keySuffix)
	total += uvarintLen(uint64(len(ent.Value)))
	total += len(ent.Value)
	return total
}

func uvarintLen(x uint64) int {
	buf := [binary.MaxVarintLen64]byte{}
	return binary.PutUvarint(buf[:], x)
}

func commonPrefix(a, b []byte) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
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
