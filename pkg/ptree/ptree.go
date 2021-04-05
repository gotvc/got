package ptree

import (
	"context"
	"io"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/refs"
	"github.com/pkg/errors"
)

type Builder struct {
	s      cadata.Store
	levels []*StreamWriter
	isDone bool
	root   *Root

	ctx context.Context
}

func NewBuilder(s cadata.Store) *Builder {
	b := &Builder{
		s: s,
	}
	b.levels = []*StreamWriter{
		b.getWriter(0),
	}
	return b
}

func (b *Builder) getWriter(i int) *StreamWriter {
	return NewStreamWriter(b.s, func(idx Index) error {
		switch {
		case b.isDone && i == len(b.levels)-1:
			b.root = &Root{
				Ref:   idx.Ref,
				Depth: uint(i),
			}
			return nil
		case i == len(b.levels)-1:
			b.levels = append(b.levels, b.getWriter(i+1))
			fallthrough
		default:
			return b.levels[i+1].Append(b.ctx, Entry{
				Key:   idx.First,
				Value: refs.MarshalRef(idx.Ref),
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
		ref, err := refs.Post(ctx, b.s, nil)
		if err != nil {
			return nil, err
		}
		b.root = &Root{Ref: *ref, Depth: 0}
	}
	return b.root, nil
}

type Iterator struct {
	s      cadata.Store
	levels []*StreamReader
	span   Span
}

func NewIterator(s cadata.Store, root Root, span Span) *Iterator {
	levels := make([]*StreamReader, root.Depth+1)
	levels[root.Depth] = NewStreamReader(s, root.Ref)
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
	ref, err := refs.ParseRef(ent.Value)
	if err != nil {
		return nil, err
	}
	it.levels[depth] = NewStreamReader(it.s, ref)
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

func (it *Iterator) Seek(ctx context.Context, k []byte) error {
	for i := range it.levels {
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

// Mutation represents a mutation to the tree
// if there is nothing in the Span, Fn will be called once with nil
// otherwise Fn will be called once for every item in the Span.
type Mutation struct {
	Span Span
	Fn   func(*Entry) []Entry
}

func putMutation(k, v []byte) Mutation {
	return Mutation{
		Span: SingleItemSpan(k),
		Fn: func(x *Entry) []Entry {
			return []Entry{{Key: k, Value: v}}
		},
	}
}

func deleteMutation(k []byte) Mutation {
	return Mutation{
		Span: SingleItemSpan(k),
		Fn: func(x *Entry) []Entry {
			return nil
		},
	}
}

type editor struct {
	s cadata.Store

	root Root
	span Span
	fn   func(*Entry) []Entry

	editors    []*StreamEditor
	newIndexes [][]Index

	ctx context.Context
}

func newEditor(s cadata.Store, root Root, mut Mutation) *editor {
	e := &editor{
		s: s,

		root: root,
		span: mut.Span,
		fn:   mut.Fn,

		editors:    make([]*StreamEditor, root.Depth+1),
		newIndexes: make([][]Index, root.Depth+1),
	}
	return e
}

func (e *editor) getStreamEditor(level int) (ret *StreamEditor) {
	if e.editors[level] != nil {
		return e.editors[level]
	}
	defer func() { e.editors[level] = ret }()

	onIndex := func(idx Index) error {
		e.newIndexes[level] = append(e.newIndexes[level], idx)
		return nil
	}
	if level == 0 {
		return NewStreamEditor(e.s, e.span, func(ent *Entry) ([]Entry, error) {
			return e.fn(ent), nil
		}, onIndex)
	}
	span := Span{}
	return NewStreamEditor(e.s, span, func(ent *Entry) ([]Entry, error) {
		se2 := e.getStreamEditor(level - 1)
		if ent != nil {
			ref, err := refs.ParseRef(ent.Value)
			if err != nil {
				return nil, err
			}
			idx := Index{First: ent.Key, Ref: ref}
			if err := se2.Process(e.ctx, idx); err != nil {
				return nil, err
			}
		}
		idxs := e.newIndexes[level-1]
		e.newIndexes[level-1] = nil
		return indexesToEntries(idxs), nil
	}, onIndex)
}

func (e *editor) run(ctx context.Context) (*Root, error) {
	e.ctx = ctx
	defer func() { e.ctx = nil }()
	se := e.getStreamEditor(int(e.root.Depth))
	// put the root through, and flush
	if err := se.Process(ctx, Index{Ref: e.root.Ref}); err != nil {
		return nil, err
	}
	for i := len(e.editors) - 1; i >= 0; i-- {
		if err := e.editors[i].Flush(ctx); err != nil {
			return nil, err
		}
	}
	// write up the levels
	for i := range e.newIndexes {
		if len(e.newIndexes[i]) > 0 && i < len(e.newIndexes)-1 {
			idxs, err := writeIndexes(ctx, e.s, e.newIndexes[i])
			if err != nil {
				return nil, err
			}
			e.newIndexes[i] = nil
			e.newIndexes[i+1] = append(e.newIndexes[i+1], idxs...)
		}
	}
	level := len(e.newIndexes)
	finalIdxs := e.newIndexes[len(e.newIndexes)-1]
	for len(finalIdxs) > 1 {
		var err error
		if finalIdxs, err = writeIndexes(ctx, e.s, finalIdxs); err != nil {
			return nil, err
		}
		level++
	}
	return &Root{Ref: finalIdxs[0].Ref, Depth: uint(level) - 1}, nil
}

func indexToEntry(idx Index) Entry {
	return Entry{
		Key:   idx.First,
		Value: refs.MarshalRef(idx.Ref),
	}
}

func indexesToEntries(idxs []Index) []Entry {
	var ents []Entry
	for _, idx := range idxs {
		ents = append(ents, indexToEntry(idx))
	}
	return ents
}

func Mutate(ctx context.Context, s cadata.Store, root Root, m Mutation) (*Root, error) {
	e := newEditor(s, root, m)
	return e.run(ctx)
}

func writeIndexes(ctx context.Context, s cadata.Store, idxs []Index) ([]Index, error) {
	var ret []Index
	w := NewStreamWriter(s, func(idx Index) error {
		ret = append(ret, idx)
		return nil
	})
	for _, idx := range idxs {
		if err := w.Append(ctx, indexToEntry(idx)); err != nil {
			return nil, err
		}
	}
	if err := w.Flush(ctx); err != nil {
		return nil, err
	}
	return ret, nil
}
