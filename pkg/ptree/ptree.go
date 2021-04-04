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
	return NewStreamWriter(b.s, func(ctx context.Context, ref refs.Ref, firstKey []byte) error {
		switch {
		case b.isDone && i == len(b.levels)-1:
			b.root = &Root{
				Ref:   ref,
				Depth: uint(i),
			}
			return nil
		case i == len(b.levels)-1:
			b.levels = append(b.levels, b.getWriter(i+1))
			fallthrough
		default:
			return b.levels[i+1].Append(ctx, Entry{
				Key:   firstKey,
				Value: refs.MarshalRef(ref),
			})
		}
	})
}

func (b *Builder) Put(ctx context.Context, key, value []byte) error {
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

// type editor struct {
// 	s      cadata.Store
// 	span   Span
// 	root   Root
// 	levels []*StreamEditor
// }

// in the editFn, need to parseRef then feed to the next editor down.
// at the bottom (level 0), apply the mutator that was given.
// when a new ref is produced, feed it back up towards the root.
// to kick the whole thing off, call Process on the root editor.

// func newEditor(s cadata.Store, root Root, mut Mutation) *editor {
// 	levels := make([]*StreamEditor, root.Depth)
// 	for i := range levels {

// 		editFn := func(*Entry) ([]Entry, error) {

// 		}
// 		onRef := func(ctx context.Context, ref refs.Ref, firstKey []byte) error {

// 		}
// 		levels[i] = NewStreamEditor(s, mut.Span, editFn, onRef)
// 	}
// 	return &editor{
// 		levels: make([]*StreamEditor, root.Depth),
// 	}
// }

// func (e *editor) run(ctx context.Context) error {
// 	e.levels[]
// }

// func (e *editor) getStreamEditor(level int) *StreamEditor {
// 	if level == len(e.levels) {
// 		NewStreamEditor(e.span, func(Entry) (Entry, error) {

// 		})
// 		return e.root.Ref
// 	}

// }

func Mutate(ctx context.Context, s cadata.Store, root Root, m Mutation) (*Root, error) {
	var ret *Root
	if root.Depth == 0 {
		se := NewStreamEditor(s, m.Span, func(ent *Entry) ([]Entry, error) {
			return m.Fn(ent), nil
		}, func(ctx context.Context, ref Ref, firstKey []byte) error {
			if ret != nil {
				panic("not implemented")
			}
			ret = &Root{Ref: ref}
			return nil
		})
		if err := se.Process(ctx, root.Ref); err != nil {
			return nil, err
		}
		if err := se.Finish(ctx); err != nil {
			return nil, err
		}
		return ret, nil
	}
	panic("not implemented")
	return nil, nil
}
