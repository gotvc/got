package ptree

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv/kv"
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

func (r *blobReader) Seek(ctx context.Context, gteq []byte) error {
	if bytes.Compare(gteq, r.prevKey) < 0 {
		r.prevKey = append(r.prevKey[:0], r.firstKey...)
	}
	var ent Entry
	for {
		if err := r.Peek(ctx, &ent); err != nil {
			if err == kv.EOS {
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
		return kv.EOS
	}
	return readEntry(ent, &r.br, r.prevKey, r.br.Len())
}

func (r *blobReader) setPrevKey(x []byte) {
	r.prevKey = append(r.prevKey[:0], x...)
}

type StreamReader struct {
	s   cadata.Store
	op  *gdat.Operator
	idx Index
	br  *blobReader
}

func NewStreamReader(s cadata.Store, op *gdat.Operator, idx Index) *StreamReader {
	return &StreamReader{
		s:   s,
		op:  op,
		idx: idx,
	}
}

func (r *StreamReader) Next(ctx context.Context, ent *Entry) error {
	br, err := r.getBlobReader(ctx)
	if err != nil {
		return err
	}
	return br.Next(ctx, ent)
}

func (r *StreamReader) Peek(ctx context.Context, ent *Entry) error {
	br, err := r.getBlobReader(ctx)
	if err != nil {
		return err
	}
	return br.Peek(ctx, ent)
}

func (r *StreamReader) Seek(ctx context.Context, gteq []byte) error {
	br, err := r.getBlobReader(ctx)
	if err != nil {
		return err
	}
	return br.Seek(ctx, gteq)
}

func (r *StreamReader) getBlobReader(ctx context.Context) (*blobReader, error) {
	if r.br == nil {
		err := r.op.GetF(ctx, r.s, r.idx.Ref, func(data []byte) error {
			data2 := append([]byte{}, data...)
			br := newBlobReader(r.idx.First, data2)
			r.br = &br
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return r.br, nil
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
	//log.Printf("append key=%q prevKey=%q firstKey=%q isFirst=%t entryLen=%d buf=%d", ent.Key, w.prevKey, w.firstKey, w.firstKey == nil, entryLen, w.Buffered())
	if entryLen+w.buf.Len() > w.maxSize {
		if err := w.Flush(ctx); err != nil {
			return err
		}
	}

	// TODO: remove this and just write to the underlying buffer
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

type StreamMerger struct {
	streams []kv.Iterator
}

func NewStreamMerger(s cadata.Store, streams []kv.Iterator) *StreamMerger {
	return &StreamMerger{
		streams: streams,
	}
}

func (sm *StreamMerger) Next(ctx context.Context, ent *Entry) error {
	sr, err := sm.selectStream(ctx)
	if err != nil {
		return err
	}
	if err := sr.Next(ctx, ent); err != nil {
		return err
	}
	if err != nil {
		return err
	}
	return sm.advancePast(ctx, ent.Key)
}

func (sm *StreamMerger) Peek(ctx context.Context, ent *Entry) error {
	sr, err := sm.selectStream(ctx)
	if err != nil {
		return err
	}
	return sr.Peek(ctx, ent)
}

func (sm *StreamMerger) advancePast(ctx context.Context, key []byte) error {
	var ent Entry
	for _, sr := range sm.streams {
		if err := sr.Peek(ctx, &ent); err != nil {
			if err == kv.EOS {
				continue
			}
			return err
		}
		// if the stream is behind, advance it.
		if bytes.Compare(ent.Key, key) <= 0 {
			if err := sr.Next(ctx, &ent); err != nil {
				return err
			}
		}
	}
	return nil
}

// selectStream will never return an ended stream
func (sm *StreamMerger) selectStream(ctx context.Context) (kv.Iterator, error) {
	var minKey []byte
	nextIndex := len(sm.streams)
	var ent Entry
	for i, sr := range sm.streams {
		if err := sr.Peek(ctx, &ent); err != nil {
			if err == kv.EOS {
				continue
			}
			return nil, err
		}
		if minKey == nil || bytes.Compare(ent.Key, minKey) <= 0 {
			minKey = ent.Key
			nextIndex = i
		}
	}
	if nextIndex < len(sm.streams) {
		return sm.streams[nextIndex], nil
	}
	return nil, io.EOF
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
