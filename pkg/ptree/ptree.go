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
	levels[root.Depth] = NewStreamReader(s, Index{Ref: root.Ref})
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
	ref, err := gdat.ParseRef(ent.Value)
	if err != nil {
		return nil, err
	}
	it.levels[depth] = NewStreamReader(it.s, Index{Ref: *ref})
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

// Mutate applies the mutation mut, to the tree root.
func Mutate(ctx context.Context, s cadata.Store, op *gdat.Operator, root Root, mut Mutation) (*Root, error) {
	b := NewBuilder(s, op)
	fnCalled := false
	mut2 := Mutation{
		Span: mut.Span,
		Fn: func(x *Entry) []Entry {
			if x == nil && fnCalled {
				return nil
			}
			fnCalled = true
			return mut.Fn(x)
		},
	}
	if err := mutate(ctx, b, Index{Ref: root.Ref}, int(root.Depth), mut2); err != nil {
		return nil, err
	}
	if !fnCalled {
		for _, ent := range mut.Fn(nil) {
			b.Put(ctx, ent.Key, ent.Value)
		}
	}
	return b.Finish(ctx)
}

func mutate(ctx context.Context, b *Builder, idx Index, depth int, mut Mutation) error {
	if depth == 0 {
		return mutateEntries(ctx, b, idx, mut)
	}
	return mutateTree(ctx, b, idx, depth, mut)
}

// mutateTree
// index at depth d has references to d - 1, where 0 is data.
func mutateTree(ctx context.Context, b *Builder, idx Index, depth int, mut Mutation) error {
	fnCalled := false
	sr := NewStreamReader(b.s, idx)
	for {
		ent, err := sr.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		ref, err := gdat.ParseRef(ent.Value)
		if err != nil {
			return err
		}
		idx2 := Index{First: ent.Key, Ref: *ref}
		if mut.Span.LessThan(ent.Key) && fnCalled {
			// The entry references data after the span, just copy it.
			if err := copyTree(ctx, b, depth, idx2); err != nil {
				return err
			}
			continue
		}
		// at this point the first entry must be <= the span
		ent2, err := sr.Peek(ctx)
		if err != nil && err != io.EOF {
			return err
		}
		if err == nil && mut.Span.GreaterThan(ent2.Key) {
			// the mutation cannot apply to the first entry because the second entry is <= it.
			if err := copyTree(ctx, b, depth, idx2); err != nil {
				return err
			}
			continue
		}
		// apply the mutation.
		if err := mutate(ctx, b, idx2, depth-1, mut); err != nil {
			return err
		}
	}
	return nil
}

func mutateEntries(ctx context.Context, b *Builder, target Index, mut Mutation) error {
	fnCalled := false
	fn := func(ent *Entry) []Entry {
		fnCalled = true
		return mut.Fn(ent)
	}
	sr := NewStreamReader(b.s, target)
	for {
		inEnt, err := sr.Next(ctx)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return err
		}
		if mut.Span.LessThan(inEnt.Key) && !fnCalled {
			outEnts := fn(nil)
			for _, outEnt := range outEnts {
				if err := b.Put(ctx, outEnt.Key, outEnt.Value); err != nil {
					return err
				}
			}
		}
		if mut.Span.Contains(inEnt.Key) {
			outEnts := fn(inEnt)
			for _, outEnt := range outEnts {
				if err := b.Put(ctx, outEnt.Key, outEnt.Value); err != nil {
					return err
				}
			}
		} else {
			if err := b.Put(ctx, inEnt.Key, inEnt.Value); err != nil {
				return err
			}
		}
	}
}

func copyTree(ctx context.Context, b *Builder, depth int, idx Index) error {
	if depth == 0 {
		panic("copyTree with depth=0 ")
	}
	if b.SyncedBelow(depth) {
		return b.CopyTree(ctx, idx, depth)
	}
	if depth == 1 {
		return copyEntries(ctx, b, idx)
	}

	sr := NewStreamReader(b.s, idx)
	for {
		ent, err := sr.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		idx2, err := entryToIndex(*ent)
		if err != nil {
			return err
		}
		if err := copyTree(ctx, b, depth-1, idx2); err != nil {
			return err
		}
	}
	return nil
}

// copyEntries resolves index (which should be depth=1), and writes each entry to b
func copyEntries(ctx context.Context, b *Builder, idx Index) error {
	sr := NewStreamReader(b.s, idx)
	for {
		ent, err := sr.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if err := b.Put(ctx, ent.Key, ent.Value); err != nil {
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

func indexToEntry(idx Index) Entry {
	return Entry{
		Key:   idx.First,
		Value: gdat.MarshalRef(idx.Ref),
	}
}

func entryToIndex(ent Entry) (Index, error) {
	ref, err := gdat.ParseRef(ent.Value)
	if err != nil {
		return Index{}, err
	}
	return Index{First: ent.Key, Ref: *ref}, nil
}
