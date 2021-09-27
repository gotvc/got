package ptree

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
	"github.com/minio/highwayhash"
	"github.com/pkg/errors"
)

type Ref = gdat.Ref

type Index struct {
	Ref   Ref
	First []byte
}

type blobReader struct {
	br       bytes.Reader
	firstKey []byte
	prevKey  []byte
}

// newBlobReader reads the entries from data, using firstKey as the first key
// firstKey and data must not be modified while using the blobReader
func newBlobReader(firstKey []byte, data []byte) blobReader {
	return blobReader{
		br:       *bytes.NewReader(data),
		firstKey: firstKey,
		prevKey:  append([]byte{}, firstKey...),
	}
}

func (r *blobReader) SeekIndexes(ctx context.Context, gteq []byte) error {
	var ent Entry
	for {
		// if the prevKey is already <= gteq, then don't bother with this
		if bytes.Compare(r.prevKey, gteq) <= 0 {
			return nil
		}
		// check to see if the next key is also <= gteq
		if err := r.Peek(ctx, &ent); err != nil {
			return err
		}
		if bytes.Compare(ent.Key, gteq) >= 0 {
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
		if bytes.Compare(ent.Key, gteq) >= 0 {
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
	s         cadata.Store
	op        *gdat.Operator
	idxs      []Index
	nextIndex int
	br        *blobReader
}

func NewStreamReader(s cadata.Store, op *gdat.Operator, idxs []Index) *StreamReader {
	for i := 0; i < len(idxs)-1; i++ {
		if bytes.Compare(idxs[i].First, idxs[i+1].First) >= 0 {
			panic("StreamReader: unordered indexes")
		}
	}
	return &StreamReader{
		s:    s,
		op:   op,
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
	var br blobReader
	err := r.op.GetF(ctx, r.s, idx.Ref, func(data []byte) error {
		data2 := append([]byte{}, data...)
		br = newBlobReader(idx.First, data2)
		return nil
	})
	return &br, err
}

type IndexHandler = func(Index) error

type StreamWriter struct {
	s       cadata.Store
	op      *gdat.Operator
	onIndex IndexHandler

	seed             []byte
	avgSize, maxSize int
	buf              bytes.Buffer

	firstKey []byte
	prevKey  []byte
	ctx      context.Context
}

func NewStreamWriter(s cadata.Store, op *gdat.Operator, avgSize, maxSize int, seed []byte, onIndex IndexHandler) *StreamWriter {
	if len(seed) > 32 {
		panic("len(seed) must be <= 32")
	}
	seed = append([]byte{}, seed...)
	for len(seed) < 32 {
		seed = append(seed, 0x00)
	}
	w := &StreamWriter{
		s:       s,
		op:      op,
		onIndex: onIndex,

		seed:    seed,
		avgSize: avgSize,
		maxSize: maxSize,
	}
	return w
}

func (w *StreamWriter) Append(ctx context.Context, ent Entry) error {
	w.ctx = ctx
	defer func() { w.ctx = nil }()

	if w.prevKey != nil && bytes.Compare(ent.Key, w.prevKey) <= 0 {
		panic(fmt.Sprintf("out of order key: prev=%q key=%q", w.prevKey, ent.Key))
	}
	entryLen := w.computeEntryLen(ent)
	if entryLen > w.maxSize {
		return errors.Errorf("entry (size=%d) exceeds maximum size %d", entryLen, w.maxSize)
	}
	if entryLen+w.buf.Len() > w.maxSize {
		if err := w.Flush(ctx); err != nil {
			return err
		}
	}

	offset := w.buf.Len()
	if w.firstKey == nil {
		if w.buf.Len() > 0 {
			panic("writeFirst called with partially full chunker")
		}
		if w.firstKey != nil {
			panic("w.firstKey should be nil")
		}
		w.firstKey = append([]byte{}, ent.Key...)
		// the first key is always fully compressed.  It is provided from the layer above.
		writeEntry(&w.buf, w.firstKey, ent)
	} else {
		writeEntry(&w.buf, w.prevKey, ent)
	}
	if w.splitAfter(w.buf.Bytes()[offset:]) {
		if err := w.Flush(ctx); err != nil {
			return err
		}
	}
	w.setPrevKey(ent.Key)
	return nil
}

func (w *StreamWriter) Buffered() int {
	return w.buf.Len()
}

func (w *StreamWriter) Flush(ctx context.Context) error {
	w.ctx = ctx
	defer func() { w.ctx = nil }()
	if w.Buffered() == 0 {
		if w.firstKey != nil {
			panic("StreamWriter: firstKey set for empty buffer")
		}
		return nil
	}
	ref, err := w.op.Post(ctx, w.s, w.buf.Bytes())
	if err != nil {
		return err
	}
	if err := w.onIndex(Index{
		First: w.firstKey,
		Ref:   *ref,
	}); err != nil {
		return err
	}
	w.firstKey = nil
	w.buf.Reset()
	return nil
}

func (w *StreamWriter) setPrevKey(k []byte) {
	w.prevKey = append(w.prevKey[:0], k...)
}

func (w *StreamWriter) splitAfter(data []byte) bool {
	r := highwayhash.Sum64(data, w.seed)
	prob := math.MaxUint64 / uint64(w.avgSize) * uint64(len(data))
	return r < prob
}

func (w *StreamWriter) computeEntryLen(ent Entry) int {
	prevKey := w.prevKey
	if w.firstKey == nil {
		prevKey = ent.Key
	}
	return computeEntryLen(prevKey, ent)
}

const maxKeySize = 4096

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
	out.Key, err = readLPBytes(out.Key, br, maxKeySize)
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
