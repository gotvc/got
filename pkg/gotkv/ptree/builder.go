package ptree

import (
	"context"
	"fmt"
)

type Builder[Ref any] struct {
	s                 Poster[Ref]
	meanSize, maxSize int
	seed              *[16]byte
	newEncoder        func() Encoder
	appendRef         func([]byte, Ref) []byte

	levels []*StreamWriter[Ref]
	isDone bool
	root   *Root[Ref]

	ctx context.Context
}

type BuilderParams[Ref any] struct {
	Store      Poster[Ref]
	MeanSize   int
	MaxSize    int
	Seed       *[16]byte
	Compare    CompareFunc
	NewEncoder func() Encoder
	AppendRef  func(out []byte, ref Ref) []byte
}

func NewBuilder[Ref any](params BuilderParams[Ref]) *Builder[Ref] {
	b := &Builder[Ref]{
		s:          params.Store,
		meanSize:   params.MeanSize,
		maxSize:    params.MaxSize,
		seed:       params.Seed,
		newEncoder: params.NewEncoder,
		appendRef:  params.AppendRef,
	}
	b.levels = []*StreamWriter[Ref]{
		b.makeWriter(0),
	}
	return b
}

func (b *Builder[Ref]) makeWriter(i int) *StreamWriter[Ref] {
	params := StreamWriterParams[Ref]{
		Store:    b.s,
		MaxSize:  b.maxSize,
		MeanSize: b.meanSize,
		Seed:     b.seed,
		Encoder:  b.newEncoder(),
		OnIndex: func(idx Index[Ref]) error {
			if b.isDone && i == len(b.levels)-1 {
				b.root = &Root[Ref]{
					Ref:   idx.Ref,
					First: append([]byte{}, idx.First...),
					Depth: uint8(i),
				}
				return nil
			}
			refBytes := make([]byte, 0, 64)
			refBytes = b.appendRef(refBytes, idx.Ref)
			if len(refBytes) > MaxRefSize {
				return fmt.Errorf("marshaled Ref is too large. size=%d MaxRefSize=%d", len(refBytes), MaxRefSize)
			}
			return b.getWriter(i+1).Append(b.ctx, Entry{
				Key:   idx.First,
				Value: refBytes,
			})
		},
	}
	return NewStreamWriter(params)
}

func (b *Builder[Ref]) getWriter(level int) *StreamWriter[Ref] {
	for len(b.levels) <= level {
		i := len(b.levels)
		b.levels = append(b.levels, b.makeWriter(i))
	}
	return b.levels[level]
}

func (b *Builder[Ref]) Put(ctx context.Context, key, value []byte) error {
	return b.put(ctx, 0, key, value)
}

func (b *Builder[Ref]) put(ctx context.Context, level int, key, value []byte) error {
	b.ctx = ctx
	defer func() { b.ctx = nil }()
	if b.isDone {
		return fmt.Errorf("builder is closed")
	}
	if b.syncLevel() < level {
		return fmt.Errorf("cannot put at level %d", level)
	}
	err := b.getWriter(level).Append(ctx, Entry{
		Key:   key,
		Value: value,
	})
	if err != nil {
		return err
	}
	return nil
}

func (b *Builder[Ref]) Finish(ctx context.Context) (*Root[Ref], error) {
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
		ref, err := b.s.Post(ctx, nil)
		if err != nil {
			return nil, err
		}
		b.root = &Root[Ref]{Ref: ref, Depth: 1}
	}
	return b.root, nil
}

func (b *Builder[Ref]) syncLevel() int {
	for i := range b.levels {
		if b.levels[i].Buffered() > 0 {
			return i
		}
	}
	return MaxTreeDepth - 1 // allow copying at any depth
}
