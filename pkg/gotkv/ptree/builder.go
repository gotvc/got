package ptree

import (
	"context"
	"errors"
	"fmt"
)

type Builder[T, Ref any] struct {
	p BuilderParams[T, Ref]

	levels []builderLevel[T, Ref]
	isDone bool
	root   *Root[T, Ref]
	ctx    context.Context
}

type builderLevel[T, Ref any] struct {
	EntryWriter *StreamWriter[T, Ref]
	IndexWriter *StreamWriter[Index[T, Ref], Ref]
}

func (bl builderLevel[T, Ref]) Append(ctx context.Context, x dual[T, Ref]) error {
	if bl.EntryWriter != nil {
		return bl.EntryWriter.Append(ctx, *x.Entry)
	} else if bl.IndexWriter != nil {
		return bl.IndexWriter.Append(ctx, *x.Index)
	} else {
		panic("empty builderLevel")
	}
}

func (bl builderLevel[T, Ref]) Buffered() (ret int) {
	if bl.EntryWriter != nil {
		return bl.EntryWriter.Buffered()
	}
	if bl.IndexWriter != nil {
		return bl.IndexWriter.Buffered()
	}
	return 0
}

type BuilderParams[T, Ref any] struct {
	Store           Poster[Ref]
	MeanSize        int
	MaxSize         int
	Seed            *[16]byte
	Compare         CompareFunc[T]
	NewEncoder      func() Encoder[T]
	NewIndexEncoder func() Encoder[Index[T, Ref]]
	Copy            func(dst *T, src T)
}

func NewBuilder[T, Ref any](params BuilderParams[T, Ref]) *Builder[T, Ref] {
	b := &Builder[T, Ref]{
		p: params,
	}
	return b
}

func (b *Builder[T, Ref]) makeLevel(i int) builderLevel[T, Ref] {
	if i == 0 {
		sw := NewStreamWriter(StreamWriterParams[T, Ref]{
			Store:    b.p.Store,
			MaxSize:  b.p.MaxSize,
			MeanSize: b.p.MeanSize,
			Seed:     b.p.Seed,
			Encoder:  b.p.NewEncoder(),
			Copy:     b.p.Copy,
			Compare:  b.p.Compare,
			OnIndex: func(idx Index[T, Ref]) error {
				if b.isDone && i == len(b.levels)-1 {
					b.root = &Root[T, Ref]{
						Index: idx,
						Depth: uint8(i),
					}
					return nil
				}
				bl := b.getLevel(i + 1)
				return bl.IndexWriter.Append(b.ctx, idx)
			},
		})
		return builderLevel[T, Ref]{
			EntryWriter: sw,
		}
	} else {
		sw := NewStreamWriter(StreamWriterParams[Index[T, Ref], Ref]{
			Store:    b.p.Store,
			MaxSize:  b.p.MaxSize,
			MeanSize: b.p.MeanSize,
			Seed:     b.p.Seed,
			Encoder:  b.p.NewIndexEncoder(),
			Copy:     upgradeCopy[T, Ref](b.p.Copy),
			Compare:  upgradeCompare[T, Ref](b.p.Compare),
			OnIndex: func(idx Index[Index[T, Ref], Ref]) error {
				idx2 := FlattenIndex(idx)
				if b.isDone && i == len(b.levels)-1 {
					root := indexToRoot(idx2, uint8(i))
					b.root = &root
					return nil
				}
				bl := b.getLevel(i + 1)
				return bl.IndexWriter.Append(b.ctx, idx2)
			},
		})
		return builderLevel[T, Ref]{
			IndexWriter: sw,
		}
	}
}

func (b *Builder[T, Ref]) getLevel(level int) builderLevel[T, Ref] {
	if level > MaxTreeDepth {
		panic("max tree depth exceeded")
	}
	for len(b.levels) <= level {
		i := len(b.levels)
		b.levels = append(b.levels, b.makeLevel(i))
	}
	return b.levels[level]
}

func (b *Builder[T, Ref]) Put(ctx context.Context, x T) error {
	return b.put(ctx, 0, dual[T, Ref]{Entry: &x})
}

func (b *Builder[T, Ref]) put(ctx context.Context, level int, x dual[T, Ref]) error {
	b.ctx = ctx
	defer func() { b.ctx = nil }()
	if b.isDone {
		return fmt.Errorf("builder is closed")
	}
	if b.syncLevel() < level {
		return fmt.Errorf("cannot put at level %d", level)
	}
	if level > 0 && !x.Index.IsNatural {
		return errors.New("cannot copy index with IsNatural=false")
	}
	return b.getLevel(level).Append(ctx, x)
}

func (b *Builder[T, Ref]) Finish(ctx context.Context) (*Root[T, Ref], error) {
	b.ctx = ctx
	defer func() { b.ctx = nil }()

	if b.isDone {
		return nil, fmt.Errorf("builder is closed")
	}
	b.isDone = true
	for i := 0; i < len(b.levels); i++ {
		bl := b.levels[i]
		if i == 0 {
			if err := bl.EntryWriter.Flush(ctx); err != nil {
				return nil, err
			}
		} else {
			if err := bl.IndexWriter.Flush(ctx); err != nil {
				return nil, err
			}
		}
	}
	// handle empty root
	if b.root == nil {
		ref, err := b.p.Store.Post(ctx, nil)
		if err != nil {
			return nil, err
		}
		b.root = &Root[T, Ref]{Index: Index[T, Ref]{Ref: ref}, Depth: 0}
	}
	return b.root, nil
}

func (b *Builder[T, Ref]) syncLevel() int {
	for i := range b.levels {
		if b.levels[i].Buffered() > 0 {
			return i
		}
	}
	return MaxTreeDepth - 1 // allow copying at any depth
}
