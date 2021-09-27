package ptree

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
	"github.com/pkg/errors"
)

const maxTreeDepth = 255

// Root is the root of the tree
type Root struct {
	Ref   Ref    `json:"ref"`
	Depth uint8  `json:"depth"`
	First []byte `json:"first,omitempty"`
}

func ParseRoot(x []byte) (*Root, error) {
	br := bytes.NewReader(x)
	refData, err := readLPBytes(nil, br, gdat.MaxRefBinaryLen)
	if err != nil {
		return nil, err
	}
	ref, err := gdat.ParseRef(refData)
	if err != nil {
		return nil, err
	}
	depth, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, err
	}
	if depth > maxTreeDepth {
		return nil, errors.Errorf("tree exceeds max tree depth (%d > %d)", depth, maxTreeDepth)
	}
	first, err := readLPBytes(nil, br, maxKeySize)
	if err != nil {
		return nil, err
	}
	return &Root{
		Ref:   *ref,
		Depth: uint8(depth),
		First: first,
	}, nil
}

func MarshalRoot(r Root) []byte {
	buf := &bytes.Buffer{}
	if err := writeLPBytes(buf, gdat.MarshalRef(r.Ref)); err != nil {
		panic(err)
	}
	if err := writeUvarint(buf, uint64(r.Depth)); err != nil {
		panic(err)
	}
	if err := writeLPBytes(buf, r.First); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

type Builder struct {
	s                cadata.Store
	op               *gdat.Operator
	avgSize, maxSize int

	levels []*StreamWriter
	isDone bool
	root   *Root

	ctx context.Context
}

func NewBuilder(s cadata.Store, op *gdat.Operator, avgSize, maxSize int, seed []byte) *Builder {
	b := &Builder{
		s:       s,
		op:      op,
		avgSize: avgSize,
		maxSize: maxSize,
	}
	b.levels = []*StreamWriter{
		b.makeWriter(0),
	}
	return b
}

func (b *Builder) makeWriter(i int) *StreamWriter {
	return NewStreamWriter(b.s, b.op, b.avgSize, b.maxSize, nil, func(idx Index) error {
		switch {
		case b.isDone && i == len(b.levels)-1:
			b.root = &Root{
				Ref:   idx.Ref,
				First: append([]byte{}, idx.First...),
				Depth: uint8(i),
			}
			return nil
		case i == len(b.levels)-1:
			b.levels = append(b.levels, b.makeWriter(i+1))
			fallthrough
		default:
			return b.levels[i+1].Append(b.ctx, Entry{
				Key:   idx.First,
				Value: gdat.MarshalRef(idx.Ref),
			})
		}
	})
}

func (b *Builder) Put(ctx context.Context, key, value []byte) error {
	b.ctx = ctx
	defer func() { b.ctx = nil }()

	if b.isDone {
		return errors.Errorf("builder is closed")
	}
	err := b.levels[0].Append(ctx, Entry{
		Key:   key,
		Value: value,
	})
	if err != nil {
		return err
	}
	return nil
}

func (b *Builder) Finish(ctx context.Context) (*Root, error) {
	b.ctx = ctx
	defer func() { b.ctx = nil }()

	if b.isDone {
		return nil, errors.Errorf("builder is closed")
	}
	b.isDone = true
	for _, w := range b.levels {
		if err := w.Flush(ctx); err != nil {
			return nil, err
		}
	}
	// handle empty root
	if b.root == nil {
		ref, err := b.op.Post(ctx, b.s, nil)
		if err != nil {
			return nil, err
		}
		b.root = &Root{Ref: *ref, Depth: 1}
	}
	return b.root, nil
}

func (b *Builder) SyncedBelow(depth int) bool {
	if len(b.levels) <= depth {
		return false
	}
	for i := range b.levels[:depth] {
		if b.levels[i].Buffered() > 0 {
			return false
		}
	}
	return true
}

// CopyTree allows writing indexes to the > 0 levels.
// An index is stored at the level above what it points to.
// Index of level 0 has depth=1
// So depth = 1 is stored in level 1.
// In order to write an index everything below the level must be synced.
// SyncedBelow(depth) MUST be true
func (b *Builder) copyTree(ctx context.Context, idx Index, depth int) error {
	if b.isDone {
		panic("builder is closed")
	}
	if depth == 0 {
		panic("CopyTree with depth=0")
	}
	if !b.SyncedBelow(depth) {
		panic("cannot copy tree; lower levels unsynced")
	}
	w := b.levels[depth]
	ent := indexToEntry(idx)
	return w.Append(ctx, ent)
}

type Iterator struct {
	s    cadata.Store
	op   *gdat.Operator
	root Root
	span Span
	srs  []*StreamReader
	pos  []byte
}

func NewIterator(s cadata.Store, op *gdat.Operator, root Root, span Span) *Iterator {
	it := &Iterator{
		s:    s,
		op:   op,
		root: root,
		span: span.Clone(),
		srs:  make([]*StreamReader, root.Depth+1),
	}
	it.setPos(span.Start)
	return it
}

func (it *Iterator) Next(ctx context.Context, ent *Entry) error {
	if err := it.initRoot(ctx); err != nil {
		return err
	}
	if err := it.withReader(ctx, 0, func(sr *StreamReader) error {
		return sr.Next(ctx, ent)
	}); err != nil {
		return err
	}
	it.setPosAfter(ent.Key)
	return it.checkAfterSpan(ent)
}

func (it *Iterator) Peek(ctx context.Context, ent *Entry) error {
	if err := it.initRoot(ctx); err != nil {
		return err
	}
	if err := it.withReader(ctx, 0, func(sr *StreamReader) error {
		return sr.Peek(ctx, ent)
	}); err != nil {
		return err
	}
	return it.checkAfterSpan(ent)
}

func (it *Iterator) Seek(ctx context.Context, gteq []byte) error {
	it.setPos(gteq)
	for i := range it.srs {
		it.srs[i] = nil
	}
	return it.initRoot(ctx)
}

func (it *Iterator) withReader(ctx context.Context, i int, fn func(sr *StreamReader) error) error {
	for {
		sr, err := it.getReader(ctx, i)
		if err != nil {
			return err
		}
		if err := fn(sr); err != nil {
			if err == kvstreams.EOS {
				it.srs[i] = nil
				continue
			}
			return err
		} else {
			return nil
		}
	}
}

func (it *Iterator) getReader(ctx context.Context, i int) (*StreamReader, error) {
	if i >= len(it.srs) {
		return nil, kvstreams.EOS
	}
	if it.srs[i] != nil {
		return it.srs[i], nil
	}
	if err := it.withReader(ctx, i+1, func(srAbove *StreamReader) error {
		idxs, err := readIndexes(ctx, srAbove)
		if err != nil {
			return err
		}
		it.srs[i+1] = nil
		it.srs[i] = NewStreamReader(it.s, it.op, idxs)
		if i == 0 {
			return it.srs[i].Seek(ctx, it.pos)
		} else {
			return it.srs[i].SeekIndexes(ctx, it.pos)
		}
	}); err != nil {
		return nil, err
	}
	return it.srs[i], nil
}

func (it *Iterator) checkAfterSpan(ent *Entry) error {
	if it.span.LessThan(ent.Key) {
		return kvstreams.EOS
	}
	return nil
}

func (it *Iterator) setPos(x []byte) {
	it.pos = append(it.pos[:0], x...)
}

func (it *Iterator) setPosAfter(x []byte) {
	it.setPos(x)
	it.pos = append(it.pos, 0x00)
}

func (it *Iterator) initRoot(ctx context.Context) error {
	i := len(it.srs) - 1
	if it.srs[i] != nil {
		return nil
	}
	it.srs[i] = NewStreamReader(it.s, it.op, []Index{rootToIndex(it.root)})
	if i == 0 {
		return it.srs[i].Seek(ctx, it.pos)
	} else {
		return it.srs[i].SeekIndexes(ctx, it.pos)
	}
}

func readIndexes(ctx context.Context, it kvstreams.Iterator) ([]Index, error) {
	var idxs []Index
	if err := kvstreams.ForEach(ctx, it, func(ent Entry) error {
		idx, err := entryToIndex(ent)
		if err != nil {
			return err
		}
		idxs = append(idxs, idx)
		return nil
	}); err != nil {
		return nil, err
	}
	return idxs, nil
}

// CopyAll copies all the entries from it to b.
func CopyAll(ctx context.Context, b *Builder, it *Iterator) error {
	var ent Entry
	for err := it.Next(ctx, &ent); err != kvstreams.EOS; err = it.Next(ctx, &ent) {
		if err != nil {
			return err
		}
		if err := b.Put(ctx, ent.Key, ent.Value); err != nil {
			return err
		}
	}
	return nil
}

// ListChildren returns the immediate children of root if any.
func ListChildren(ctx context.Context, s cadata.Store, op *gdat.Operator, root Root) ([]Index, error) {
	if PointsToEntries(root) {
		return nil, errors.Errorf("cannot list children of root with depth=%d", root.Depth)
	}
	sr := NewStreamReader(s, op, []Index{rootToIndex(root)})
	var idxs []Index
	var ent Entry
	for {
		if err := sr.Next(ctx, &ent); err != nil {
			if err == kvstreams.EOS {
				break
			}
			return nil, err
		}
		idx, err := entryToIndex(ent)
		if err != nil {
			return nil, err
		}
		idxs = append(idxs, idx)
	}
	return idxs, nil
}

// ListEntries
func ListEntries(ctx context.Context, s cadata.Store, op *gdat.Operator, idx Index) ([]Entry, error) {
	var ents []Entry
	sr := NewStreamReader(s, op, []Index{idx})
	for {
		var ent Entry
		if err := sr.Next(ctx, &ent); err != nil {
			if err == kvstreams.EOS {
				return ents, nil
			}
			return nil, err
		}
		ents = append(ents, ent)
	}
}

func PointsToEntries(root Root) bool {
	return root.Depth == 0
}

func PointsToIndexes(root Root) bool {
	return root.Depth > 0
}
