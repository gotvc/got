package ptree

import (
	"context"
	"io"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/gdat"
)

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
	if err := mutate(ctx, b, rootToIndex(root), int(root.Depth), mut2); err != nil {
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
	sr := NewStreamReader(b.s, b.op, idx)
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
	sr := NewStreamReader(b.s, b.op, target)
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
		return b.copyTree(ctx, idx, depth)
	}
	if depth == 1 {
		return copyEntries(ctx, b, idx)
	}

	sr := NewStreamReader(b.s, b.op, idx)
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
	sr := NewStreamReader(b.s, b.op, idx)
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

func indexToRoot(idx Index, depth uint8) Root {
	return Root{
		Ref:   idx.Ref,
		First: idx.First,
		Depth: depth,
	}
}

func rootToIndex(r Root) Index {
	return Index{
		Ref:   r.Ref,
		First: r.First,
	}
}
