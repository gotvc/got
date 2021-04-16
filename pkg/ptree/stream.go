package ptree

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log"

	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/chunking"
	"github.com/brendoncarroll/got/pkg/gdat"
	"github.com/brendoncarroll/got/pkg/ptree/ptreeproto"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

const (
	defaultAvgSize = 1 << 13
	defaultMaxSize = blobs.MaxSize
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
	l, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, err
	}
	entBuf := make([]byte, int(l))
	if _, err := io.ReadFull(br, entBuf); err != nil {
		return nil, err
	}
	var entproto ptreeproto.Entry
	if err := proto.Unmarshal(entBuf, &entproto); err != nil {
		return nil, err
	}
	if int(entproto.KeyBackspace) > len(prevKey) {
		return nil, errors.Errorf("backspace is > len(prevKey): prevKey=%q bs=%d", prevKey, entproto.KeyBackspace)
	}
	end := len(prevKey) - int(entproto.KeyBackspace)
	key := append([]byte{}, prevKey[:end]...)
	key = append(key, entproto.KeySuffix...)
	return &Entry{
		Key:   key,
		Value: entproto.Value,
	}, nil
}

func writeEntry(w *bytes.Buffer, prevKey []byte, ent Entry) {
	l := commonPrefix(prevKey, ent.Key)
	keySuffix := ent.Key[l:]
	backspace := uint32(len(prevKey) - l)
	data, _ := proto.Marshal(&ptreeproto.Entry{
		KeyBackspace: backspace,
		KeySuffix:    keySuffix,
		Value:        ent.Value,
	})
	lenBuf := [binary.MaxVarintLen64]byte{}
	n := binary.PutUvarint(lenBuf[:], uint64(len(data)))
	w.Write(lenBuf[:n])
	w.Write(data)
}

type StreamReader struct {
	s       cadata.Store
	op      *gdat.Operator
	idx     Index
	br      *bytes.Reader
	prevKey []byte
}

func NewStreamReader(s cadata.Store, idx Index) *StreamReader {
	return &StreamReader{
		s:   s,
		op:  gdat.NewOperator(),
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
	chunker *chunking.ContentDefined

	firstKey []byte
	prevKey  []byte
	ctx      context.Context
}

func NewStreamWriter(s cadata.Store, op *gdat.Operator, avgSize, maxSize int, onIndex IndexHandler) *StreamWriter {
	w := &StreamWriter{
		s:  s,
		op: op,
	}
	w.chunker = chunking.NewContentDefined(avgSize, maxSize, func(data []byte) error {
		if w.firstKey == nil {
			panic("firstKey should be set")
		}
		ref, err := op.Post(w.ctx, w.s, data)
		if err != nil {
			return err
		}
		idx := Index{Ref: *ref, First: w.firstKey}
		w.firstKey = nil
		return onIndex(idx)
	})
	return w
}

func (w *StreamWriter) Append(ctx context.Context, ent Entry) error {
	w.ctx = ctx
	defer func() { w.ctx = nil }()
	if w.prevKey != nil && bytes.Compare(ent.Key, w.prevKey) <= 0 {
		log.Println("prev:", string(w.prevKey), string(ent.Key))
		panic("out of order key")
	}
	buf := &bytes.Buffer{}
	writeEntry(buf, w.prevKey, ent)
	if w.chunker.WouldOverflow(buf.Bytes()) {
		if err := w.chunker.Flush(); err != nil {
			return err
		}
		return w.writeFirst(ctx, ent)
	}
	if w.firstKey == nil {
		return w.writeFirst(ctx, ent)
	}
	if err := w.chunker.WriteNoSplit(buf.Bytes()); err != nil {
		return err
	}
	w.setPrevKey(ent.Key)
	return nil
}

func (w *StreamWriter) writeFirst(ctx context.Context, ent Entry) error {
	if w.chunker.Buffered() > 0 {
		panic("writeFirst called with partially full chunker")
	}
	if w.firstKey != nil {
		panic("w.firstKey should be nil")
	}
	w.firstKey = append([]byte{}, ent.Key...)
	buf := &bytes.Buffer{}
	// the first key is always fully compressed.  It is provided from the layer above.
	writeEntry(buf, ent.Key, ent)
	if err := w.chunker.WriteNoSplit(buf.Bytes()); err != nil {
		return err
	}
	w.setPrevKey(ent.Key)
	return nil
}

func (w *StreamWriter) Buffered() int {
	return w.chunker.Buffered()
}

func (w *StreamWriter) Flush(ctx context.Context) error {
	w.ctx = ctx
	defer func() { w.ctx = nil }()
	return w.chunker.Flush()
}

func (w *StreamWriter) setPrevKey(k []byte) {
	w.prevKey = append(w.prevKey[:0], k...)
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
