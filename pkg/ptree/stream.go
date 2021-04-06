package ptree

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"log"

	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/gdat"
)

const (
	defaultAvgSize = 1 << 13
	defaultMaxSize = blobs.MaxSize
)

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

func readEntry(br *bytes.Reader) (*Entry, error) {
	l, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, err
	}
	entBuf := make([]byte, int(l))
	if _, err := io.ReadFull(br, entBuf); err != nil {
		return nil, err
	}
	var ent Entry
	if err := json.Unmarshal(entBuf, &ent); err != nil {
		return nil, err
	}
	return &ent, nil
}

func writeEntry(w *bytes.Buffer, ent Entry) {
	data, _ := json.Marshal(ent)
	buf := [binary.MaxVarintLen64]byte{}
	n := binary.PutUvarint(buf[:], uint64(len(data)))
	w.Write(buf[:n])
	w.Write(data)
}

type Index struct {
	Ref   Ref
	First []byte
}

type StreamReader struct {
	s   cadata.Store
	op  *gdat.Operator
	idx Index
	br  *bytes.Reader
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
	return readEntry(br)
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
	return readEntry(br)
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
	}
	return r.br, nil
}

type IndexHandler = func(Index) error

type StreamWriter struct {
	s       cadata.Store
	op      *gdat.Operator
	chunker *Chunker

	lastKey []byte
	ctx     context.Context
}

func NewStreamWriter(s cadata.Store, op *gdat.Operator, onIndex IndexHandler) *StreamWriter {
	w := &StreamWriter{
		s:  s,
		op: op,
	}
	w.chunker = NewChunker(defaultAvgSize, defaultMaxSize, func(data []byte) error {
		ref, err := op.Post(w.ctx, w.s, data)
		if err != nil {
			return err
		}
		br := bytes.NewReader(data)
		ent, err := readEntry(br)
		if err != nil {
			panic(err) // we just wrote this
		}
		idx := Index{Ref: *ref, First: ent.Key}
		return onIndex(idx)
	})
	return w
}

func (w *StreamWriter) Append(ctx context.Context, ent Entry) error {
	w.ctx = ctx
	defer func() { w.ctx = nil }()
	if w.lastKey != nil && bytes.Compare(ent.Key, w.lastKey) <= 0 {
		log.Println("prev:", string(w.lastKey), string(ent.Key))
		panic("out of order key")
	}
	buf := &bytes.Buffer{}
	writeEntry(buf, ent)
	w.setLastKey(ent.Key)
	return w.chunker.WriteNoSplit(buf.Bytes())
}

func (w *StreamWriter) Buffered() int {
	return w.chunker.Buffered()
}

func (w *StreamWriter) Flush(ctx context.Context) error {
	w.ctx = ctx
	defer func() { w.ctx = nil }()
	return w.chunker.Flush()
}

func (w *StreamWriter) setLastKey(k []byte) {
	w.lastKey = append(w.lastKey[:0], k...)
}

type entryMutator = func(*Entry) ([]Entry, error)

type StreamEditor struct {
	s cadata.Store

	span    Span
	fn      entryMutator
	onIndex IndexHandler

	inputRefs    map[Ref]struct{}
	w            *StreamWriter
	prevInputKey []byte
	syncCount    int
	fnCalled     bool
}

func NewStreamEditor(s cadata.Store, op *gdat.Operator, span Span, fn entryMutator, onIndex IndexHandler) *StreamEditor {
	e := &StreamEditor{
		s: s,

		span:    span,
		fn:      fn,
		onIndex: onIndex,

		inputRefs: make(map[Ref]struct{}),
	}
	e.w = NewStreamWriter(s, op, func(idx Index) error {
		if _, exists := e.inputRefs[idx.Ref]; exists {
			e.syncCount++
		} else {
			e.syncCount = 0
		}
		return e.onIndex(idx)
	})
	return e
}

// Done returns whether the editor has completed it's mutation to the stream
// if Done returns true, all future calls to Process will emit exactly the same ref as passed in.
func (e *StreamEditor) Done() bool {
	return e.prevInputKey != nil &&
		e.span.LessThan(e.prevInputKey) &&
		e.syncCount > 1
}

func (e *StreamEditor) Process(ctx context.Context, x Index) error {
	e.inputRefs[x.Ref] = struct{}{}
	defer delete(e.inputRefs, x.Ref)
	r := NewStreamReader(e.s, Index{Ref: x.Ref})
	for {
		xEnt, err := r.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if e.span.LessThan(xEnt.Key) && !e.fnCalled {
			if err := e.processEntry(ctx, nil); err != nil {
				return err
			}
		}
		// either edit the key or copy it
		if e.span.Contains(xEnt.Key) {
			if err := e.processEntry(ctx, xEnt); err != nil {
				return err
			}
		} else {
			if err := e.w.Append(ctx, *xEnt); err != nil {
				return err
			}
		}
		e.prevInputKey = xEnt.Key
	}
	return nil
}

func (e *StreamEditor) processEntry(ctx context.Context, xEnt *Entry) error {
	yEnts, err := e.fn(xEnt)
	if err != nil {
		return err
	}
	e.fnCalled = true
	for _, yEnt := range yEnts {
		if err := e.w.Append(ctx, yEnt); err != nil {
			return err
		}
	}
	return nil
}

func (e *StreamEditor) Flush(ctx context.Context) error {
	if !e.fnCalled {
		e.processEntry(ctx, nil)
	}
	return e.w.Flush(ctx)
}

type StreamMerger struct {
	s cadata.Store

	streams []*StreamReader
}

func NewStreamMerger(s cadata.Store, streams []*StreamReader) *StreamMerger {
	return &StreamMerger{
		s:       s,
		streams: streams,
	}
}

func (sm *StreamMerger) Next(ctx context.Context) (*Entry, error) {
	sr, err := sm.selectStream(ctx)
	if err != nil {
		return nil, err
	}
	return sr.Next(ctx)
}

func (sm *StreamMerger) Peek(ctx context.Context) (*Entry, error) {
	sr, err := sm.selectStream(ctx)
	if err != nil {
		return nil, err
	}
	return sr.Peek(ctx)
}

// selectStream will never return an ended stream
func (sm *StreamMerger) selectStream(ctx context.Context) (*StreamReader, error) {
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

func StreamCopy(ctx context.Context, dst *StreamWriter, src StreamIterator) error {
	for {
		ent, err := src.Next(ctx)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return err
		}
		if err := dst.Append(ctx, *ent); err != nil {
			return err
		}
	}
}
