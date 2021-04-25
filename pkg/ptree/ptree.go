package ptree

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/gdat"
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
	s      cadata.Store
	op     *gdat.Operator
	levels []*StreamReader
	span   Span
}

func NewIterator(s cadata.Store, op *gdat.Operator, root Root, span Span) *Iterator {
	levels := make([]*StreamReader, root.Depth+1)
	levels[root.Depth] = NewStreamReader(s, op, rootToIndex(root))
	return &Iterator{
		s:      s,
		op:     op,
		levels: levels,
		span:   span,
	}
}

func (it *Iterator) getReader(ctx context.Context, depth int) (*StreamReader, error) {
	if depth == len(it.levels) {
		return nil, io.EOF
	}
	if it.levels[depth] != nil {
		return it.levels[depth], nil
	}

	// create a stream reader for depth, by reading an entry from depth+1
	var ent *Entry
	for {
		sr, err := it.getReader(ctx, depth+1)
		if err != nil {
			return nil, err
		}
		ent, err = sr.Next(ctx)
		if err != nil {
			if err == io.EOF {
				it.markEOF(depth + 1)
				continue
			}
			return nil, err
		}
		break
	}
	idx, err := entryToIndex(*ent)
	if err != nil {
		return nil, err
	}
	it.levels[depth] = NewStreamReader(it.s, it.op, idx)
	return it.levels[depth], nil
}

func (it *Iterator) markEOF(depth int) {
	it.levels[depth] = nil
}

func (it *Iterator) Next(ctx context.Context) (*Entry, error) {
	for {
		sr, err := it.getReader(ctx, 0)
		if err != nil {
			return nil, err // io.EOF here is the true EOF
		}
		ent, err := sr.Next(ctx)
		if err != nil {
			if err == io.EOF {
				it.markEOF(0)
				continue
			}
			return nil, err
		}
		if it.span.Contains(ent.Key) {
			return ent, nil
		}
	}
}

func (it *Iterator) Peek(ctx context.Context) (*Entry, error) {
	for {
		sr, err := it.getReader(ctx, 0)
		if err != nil {
			return nil, err
		}
		ent, err := sr.Peek(ctx)
		if err != nil {
			if err == io.EOF {
				it.markEOF(0)
				continue
			}
			return nil, err
		}
		if it.span.Contains(ent.Key) {
			return ent, nil
		}
	}
}

func (it *Iterator) Seek(ctx context.Context, k []byte) error {
	for i := len(it.levels) - 1; i >= 0; i-- {
		sr, err := it.getReader(ctx, i)
		if err != nil {
			return err
		}
		if err := sr.Seek(ctx, k); err != nil {
			return err
		}
	}
	return nil
}

func (it *Iterator) SyncedBelow(depth int) bool {
	if depth > len(it.levels) {
		return false
	}
	for i := range it.levels[:depth] {
		if it.levels[i] != nil {
			return false
		}
	}
	return true
}

func (it *Iterator) nextIndex(ctx context.Context, depth int) (*Index, error) {
	if !it.SyncedBelow(depth) {
		panic("cannot iterate indexes. not synced")
	}
	sr, err := it.getReader(ctx, depth)
	if err != nil {
		return nil, err
	}
	ent, err := sr.Next(ctx)
	if err != nil {
		return nil, err
	}
	idx, err := entryToIndex(*ent)
	if err != nil {
		return nil, err
	}
	return &idx, nil
}

// CopyAll copies all the entries from it to b.
func CopyAll(ctx context.Context, b *Builder, it *Iterator) error {
	for {
		if err := func() error {
			for i := len(b.levels); i > 0; i-- {
				if !b.SyncedBelow(i) || !it.SyncedBelow(i) {
					break
				}
				if err := copyIndex(ctx, b, it, i); err != nil {
					if err == io.EOF {
						return nil
					}
					return err
				}
			}
			return copyEntry(ctx, b, it)
		}(); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func copyIndex(ctx context.Context, b *Builder, it *Iterator, depth int) error {
	idx, err := it.nextIndex(ctx, depth)
	if err != nil {
		return err
	}
	return b.copyTree(ctx, *idx, depth)
}

func copyEntry(ctx context.Context, b *Builder, it *Iterator) error {
	ent, err := it.Next(ctx)
	if err != nil {
		return err
	}
	return b.Put(ctx, ent.Key, ent.Value)
}

// ListChildren returns the immediate children of root if any.
func ListChildren(ctx context.Context, s cadata.Store, root Root) ([]Index, error) {
	if root.Depth < 1 {
		return nil, errors.Errorf("cannot list children of root with depth=%d", root.Depth)
	}
	op := gdat.NewOperator()
	sr := NewStreamReader(s, &op, rootToIndex(root))
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
func ListEntries(ctx context.Context, s cadata.Store, idx Index) ([]Entry, error) {
	var ents []Entry
	op := gdat.NewOperator()
	sr := NewStreamReader(s, &op, idx)
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
