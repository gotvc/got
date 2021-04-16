package ptree

import (
	"context"
	"io"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/gdat"
	"github.com/pkg/errors"
)

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
func (b *Builder) CopyTree(ctx context.Context, idx Index, depth int) error {
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
	levels []*StreamReader
	span   Span
}

func NewIterator(s cadata.Store, root Root, span Span) *Iterator {
	levels := make([]*StreamReader, root.Depth+1)
	levels[root.Depth] = NewStreamReader(s, rootToIndex(root))
	return &Iterator{
		s:      s,
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
	it.levels[depth] = NewStreamReader(it.s, idx)
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
	for i := len(it.levels); i >= 0; i-- {
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
