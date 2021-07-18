package ptree

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
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
	refData, err := readLPBytes(br)
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
	first, err := readLPBytes(br)
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

func NewBuilder(s cadata.Store, op *gdat.Operator) *Builder {
	b := &Builder{
		s:       s,
		op:      op,
		avgSize: defaultAvgSize,
		maxSize: defaultMaxSize,
	}
	b.levels = []*StreamWriter{
		b.makeWriter(0),
	}
	return b
}

func (b *Builder) makeWriter(i int) *StreamWriter {
	return NewStreamWriter(b.s, b.op, b.avgSize, b.maxSize, func(idx Index) error {
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
}

func NewIterator(s cadata.Store, op *gdat.Operator, root Root, span Span) *Iterator {
	it := &Iterator{
		s:    s,
		op:   op,
		root: root,
		span: span.Clone(),
	}
	return it
}

func (it *Iterator) Next(ctx context.Context) (*Entry, error) {
	ent, err := it.Peek(ctx)
	if err != nil {
		return nil, err
	}
	it.setPosAfter(ent.Key)
	return ent, nil
}

func (it *Iterator) Peek(ctx context.Context) (*Entry, error) {
	_, ent, err := it.peek(ctx)
	if err != nil {
		return nil, err
	}
	if it.span.LessThan(ent.Key) {
		return nil, io.EOF
	}
	return ent, nil
}

func (it *Iterator) peek(ctx context.Context) (int, *Entry, error) {
	return peekTree(ctx, it.s, it.op, it.root, it.span.Start)
}

func (it *Iterator) Seek(ctx context.Context, k []byte) error {
	it.setPos(k)
	return nil
}

func (it *Iterator) setPos(k []byte) {
	it.span.Start = append(it.span.Start[:0], k...)
}

func (it *Iterator) setPosAfter(k []byte) {
	it.setPos(k)
	it.span.Start = append(it.span.Start, 0x00)
}

func peekEntries(ctx context.Context, s cadata.Store, op *gdat.Operator, idx Index, gteq []byte) (*Entry, error) {
	entries, err := ListEntries(ctx, s, op, idx)
	if err != nil {
		return nil, err
	}
	for _, ent := range entries {
		// ent.Key >= gteq
		if bytes.Compare(ent.Key, gteq) >= 0 {
			return &ent, nil
		}
	}
	return nil, io.EOF
}

func peekTree(ctx context.Context, s cadata.Store, op *gdat.Operator, root Root, gteq []byte) (int, *Entry, error) {
	if root.Depth == 0 {
		idx := rootToIndex(root)
		ent, err := peekEntries(ctx, s, op, idx, gteq)
		if err != nil {
			return 0, nil, err
		}
		var syncLevel int
		if bytes.Equal(ent.Key, idx.First) {
			syncLevel = 1
		}
		return syncLevel, ent, nil
	} else {
		idxs, err := ListChildren(ctx, s, op, root)
		if err != nil {
			return 0, nil, err
		}
		for i := 0; i < len(idxs); i++ {
			// if the first element in the next is also lteq then skip.
			if i+1 < len(idxs) && bytes.Compare(idxs[i+1].First, gteq) <= 0 {
				continue
			}
			syncLevel, ent, err := peekTree(ctx, s, op, indexToRoot(idxs[i], root.Depth-1), gteq)
			if err == io.EOF {
				continue
			}
			if i == 0 {
				syncLevel++
			}
			return syncLevel, ent, err
		}
	}
	return 0, nil, io.EOF
}

// CopyAll copies all the entries from it to b.
func CopyAll(ctx context.Context, b *Builder, it *Iterator) error {
	for {
		if err := copyEntry(ctx, b, it); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
	}
	return nil
}

func copyEntry(ctx context.Context, b *Builder, it *Iterator) error {
	ent, err := it.Next(ctx)
	if err != nil {
		return err
	}
	return b.Put(ctx, ent.Key, ent.Value)
}

// ListChildren returns the immediate children of root if any.
func ListChildren(ctx context.Context, s cadata.Store, op *gdat.Operator, root Root) ([]Index, error) {
	if root.Depth < 1 {
		return nil, errors.Errorf("cannot list children of root with depth=%d", root.Depth)
	}
	sr := NewStreamReader(s, op, rootToIndex(root))
	var idxs []Index
	for {
		ent, err := sr.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		idx, err := entryToIndex(*ent)
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
	sr := NewStreamReader(s, op, idx)
	for {
		ent, err := sr.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		ents = append(ents, *ent)
	}
	return ents, nil
}
