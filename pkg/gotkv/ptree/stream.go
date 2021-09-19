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
	"github.com/minio/highwayhash"
	"github.com/pkg/errors"
)

type Ref = gdat.Ref

type Index struct {
	Ref   Ref
	First []byte
}

type StreamIterator interface {
	Next(ctx context.Context) (*Entry, error)
	Peek(ctx context.Context) (*Entry, error)
}

type StreamLiteral []Entry

func (s *StreamLiteral) Peek(ctx context.Context) (*Entry, error) {
	if s == nil || len(*s) == 0 {
		return nil, io.EOF
	}
	ent := (*s)[0]
	return &ent, nil
}

func (s *StreamLiteral) Next(ctx context.Context) (*Entry, error) {
	ent, err := s.Peek(ctx)
	if err != nil {
		return nil, err
	}
	*s = (*s)[1:]
	return ent, nil
}

func readEntry(br *bytes.Reader, prevKey []byte) (*Entry, error) {
	entBuf, err := readLPBytes(br)
	if err != nil {
		return nil, err
	}
	br = bytes.NewReader(entBuf)
	keyBackspace, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, err
	}
	keySuffix, err := readLPBytes(br)
	if err != nil {
		return nil, err
	}
	value, err := readLPBytes(br)
	if err != nil {
		return nil, err
	}
	if int(keyBackspace) > len(prevKey) {
		return nil, errors.Errorf("backspace is > len(prevKey): prevKey=%q bs=%d", prevKey, keyBackspace)
	}
	end := len(prevKey) - int(keyBackspace)
	key := append([]byte{}, prevKey[:end]...)
	key = append(key, keySuffix...)
	return &Entry{
		Key:   key,
		Value: value,
	}, nil
}

func writeEntry(w *bytes.Buffer, prevKey []byte, ent Entry) {
	l := commonPrefix(prevKey, ent.Key)
	keySuffix := ent.Key[l:]
	backspace := uint32(len(prevKey) - l)

	w2 := &bytes.Buffer{}
	if err := writeUvarint(w2, uint64(backspace)); err != nil {
		panic(err)
	}
	if err := writeLPBytes(w2, keySuffix); err != nil {
		panic(err)
	}
	if err := writeLPBytes(w2, ent.Value); err != nil {
		panic(err)
	}

	if err := writeUvarint(w, uint64(w2.Len())); err != nil {
		panic(err)
	}
	w.Write(w2.Bytes())
}

type StreamReader struct {
	s       cadata.Store
	op      *gdat.Operator
	idx     Index
	br      *bytes.Reader
	prevKey []byte
}

func NewStreamReader(s cadata.Store, op *gdat.Operator, idx Index) *StreamReader {
	return &StreamReader{
		s:   s,
		op:  op,
		idx: idx,
	}
}

func (r *StreamReader) Next(ctx context.Context) (*Entry, error) {
	br, err := r.getByteReader(ctx)
	if err != nil {
		return nil, err
	}
	ent, err := readEntry(br, r.prevKey)
	if err != nil {
		return nil, err
	}
	r.setPrevKey(ent.Key)
	return ent, nil
}

func (r *StreamReader) Peek(ctx context.Context) (*Entry, error) {
	br, err := r.getByteReader(ctx)
	if err != nil {
		return nil, err
	}
	l1 := br.Len()
	defer func() {
		l2 := br.Len()
		for i := 0; i < l1-l2; i++ {
			if err := br.UnreadByte(); err != nil {
				panic(err)
			}
		}
	}()
	return readEntry(br, r.prevKey)
}

func (r *StreamReader) Seek(ctx context.Context, k []byte) error {
	for {
		ent, err := r.Peek(ctx)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return err
		}
		if bytes.Compare(ent.Key, k) >= 0 {
			return nil
		}
		_, err = r.Next(ctx)
		if err != nil {
			return err
		}
	}
}

func (r *StreamReader) getByteReader(ctx context.Context) (*bytes.Reader, error) {
	if r.br == nil {
		err := r.op.GetF(ctx, r.s, r.idx.Ref, func(data []byte) error {
			r.br = bytes.NewReader(append([]byte{}, data...))
			return nil
		})
		if err != nil {
			return nil, err
		}
		r.setPrevKey(r.idx.First)
	}
	return r.br, nil
}

func (r *StreamReader) setPrevKey(x []byte) {
	r.prevKey = append(r.prevKey[:0], x...)
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

	buf := &bytes.Buffer{}
	if w.firstKey == nil {
		if w.buf.Len() > 0 {
			panic("writeFirst called with partially full chunker")
		}
		if w.firstKey != nil {
			panic("w.firstKey should be nil")
		}
		w.firstKey = append([]byte{}, ent.Key...)
		// the first key is always fully compressed.  It is provided from the layer above.
		writeEntry(buf, ent.Key, ent)
	} else {
		writeEntry(buf, w.prevKey, ent)
	}
	if _, err := w.buf.Write(buf.Bytes()); err != nil {
		return err
	}
	w.setPrevKey(ent.Key)
	if w.splitAfter(buf.Bytes()) {
		if err := w.Flush(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (w *StreamWriter) Buffered() int {
	return w.buf.Len()
}

func (w *StreamWriter) Flush(ctx context.Context) error {
	w.ctx = ctx
	defer func() { w.ctx = nil }()

	if w.Buffered() == 0 {
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
	buf := bytes.Buffer{}
	writeEntry(&buf, w.prevKey, ent)
	return buf.Len()
}

type StreamMerger struct {
	streams []StreamIterator
}

func NewStreamMerger(s cadata.Store, streams []StreamIterator) *StreamMerger {
	return &StreamMerger{
		streams: streams,
	}
}

func (sm *StreamMerger) Next(ctx context.Context) (*Entry, error) {
	sr, err := sm.selectStream(ctx)
	if err != nil {
		return nil, err
	}
	ent, err := sr.Next(ctx)
	if err != nil {
		return nil, err
	}
	return ent, sm.advancePast(ctx, ent.Key)
}

func (sm *StreamMerger) Peek(ctx context.Context) (*Entry, error) {
	sr, err := sm.selectStream(ctx)
	if err != nil {
		return nil, err
	}
	return sr.Peek(ctx)
}

func (sm *StreamMerger) advancePast(ctx context.Context, key []byte) error {
	for _, sr := range sm.streams {
		ent, err := sr.Peek(ctx)
		if err != nil {
			if err == io.EOF {
				continue
			}
		}
		// if the stream is behind, advance it.
		if bytes.Compare(ent.Key, key) <= 0 {
			if _, err := sr.Next(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

// selectStream will never return an ended stream
func (sm *StreamMerger) selectStream(ctx context.Context) (StreamIterator, error) {
	var minKey []byte
	nextIndex := len(sm.streams)
	for i, sr := range sm.streams {
		ent, err := sr.Peek(ctx)
		if err != nil {
			if err == io.EOF {
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

const maxKeySize = 4096

func writeUvarint(w *bytes.Buffer, x uint64) error {
	lenBuf := [binary.MaxVarintLen64]byte{}
	n := binary.PutUvarint(lenBuf[:], uint64(x))
	_, err := w.Write(lenBuf[:n])
	return err
}

func writeLPBytes(w *bytes.Buffer, x []byte) error {
	if err := writeUvarint(w, uint64(len(x))); err != nil {
		return err
	}
	_, err := w.Write(x)
	return err
}

func readLPBytes(br *bytes.Reader) ([]byte, error) {
	l, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, err
	}
	if l > maxKeySize {
		return nil, errors.Errorf("lp bytestring exceeds max size")
	}
	buf := make([]byte, int(l))
	if _, err := io.ReadFull(br, buf[:]); err != nil {
		return nil, err
	}
	return buf, nil
}
