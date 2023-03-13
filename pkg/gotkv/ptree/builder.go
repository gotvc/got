package ptree

import (
	"context"
	"fmt"
)

type Builder[T, Ref any] struct {
	p BuilderParams[T, Ref]

	levels []*StreamWriter[T, Ref]
	isDone bool
	root   *Root[T, Ref]
	ctx    context.Context
}

type BuilderParams[T, Ref any] struct {
	Store        Poster[Ref]
	MeanSize     int
	MaxSize      int
	Seed         *[16]byte
	Compare      CompareFunc[T]
	NewEncoder   func() Encoder[T]
	ConvertIndex func(idx Index[T, Ref]) T
	Copy         func(dst *T, src T)
}

func NewBuilder[Ref any](params BuilderParams[Ref]) *Builder[Ref] {
	b := &Builder[Ref]{
		p: params,
	}
	return b
}

func (b *Builder[Ref]) makeWriter(i int) *StreamWriter[Ref] {
	params := StreamWriterParams[Ref]{
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
					Ref:   idx.Ref,
					First: idx.First,
					Depth: uint8(i),
				}
				return nil
			}
			return b.getWriter(i+1).Append(b.ctx, b.p.ConvertIndex(idx))
		},
	}
	return NewStreamWriter(params)
}

func (b *Builder[T, Ref]) getWriter(level int) *StreamWriter[T, Ref] {
	for len(b.levels) <= level {
		i := len(b.levels)
		b.levels = append(b.levels, b.makeWriter(i))
	}
	return b.levels[level]
}

func (b *Builder[T, Ref]) Put(ctx context.Context, x T) error {
	return b.put(ctx, 0, x)
}

func (b *Builder[T, Ref]) put(ctx context.Context, level int, x T) error {
	b.ctx = ctx
	defer func() { b.ctx = nil }()
	if b.isDone {
		return fmt.Errorf("builder is closed")
	}
	if b.syncLevel() < level {
		return fmt.Errorf("cannot put at level %d", level)
	}
	err := b.getWriter(level).Append(ctx, x)
	if err != nil {
		return err
	}
	return nil
}

func (b *Builder[T, Ref]) Finish(ctx context.Context) (*Root[T, Ref], error) {
	b.ctx = ctx
	defer func() { b.ctx = nil }()

	if b.isDone {
		return nil, fmt.Errorf("builder is closed")
	}
	b.isDone = true
	for _, w := range b.levels {
		if err := w.Flush(ctx); err != nil {
			return nil, err
		}
	}
	// handle empty root
	if b.root == nil {
		ref, err := b.p.Store.Post(ctx, nil)
		if err != nil {
			return nil, err
		}
		b.root = &Root[T, Ref]{Ref: ref, Depth: 1}
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
