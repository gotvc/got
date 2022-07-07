package ptree

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/pkg/errors"
)

type Builder struct {
	s                cadata.Store
	op               *gdat.Operator
	avgSize, maxSize int
	seed             *[16]byte

	levels []*StreamWriter
	isDone bool
	root   *Root

	ctx context.Context
}

func NewBuilder(op *gdat.Operator, avgSize, maxSize int, seed *[16]byte, cmp CompareFunc, s cadata.Store) *Builder {
	b := &Builder{
		s:       s,
		op:      op,
		avgSize: avgSize,
		maxSize: maxSize,
		seed:    seed,
	}
	b.levels = []*StreamWriter{
		b.makeWriter(0),
	}
	return b
}

func (b *Builder) makeWriter(i int) *StreamWriter {
	return NewStreamWriter(b.s, b.op, b.avgSize, b.maxSize, b.seed, func(idx Index) error {
		if b.isDone && i == len(b.levels)-1 {
			b.root = &Root{
				Ref:   idx.Ref,
				First: append([]byte{}, idx.First...),
				Depth: uint8(i),
			}
			return nil
		}
		return b.getWriter(i+1).Append(b.ctx, Entry{
			Key:   idx.First,
			Value: gdat.MarshalRef(idx.Ref),
		})
	})
}

func (b *Builder) getWriter(level int) *StreamWriter {
	for len(b.levels) <= level {
		i := len(b.levels)
		b.levels = append(b.levels, b.makeWriter(i))
	}
	return b.levels[level]
}

func (b *Builder) Put(ctx context.Context, key, value []byte) error {
	return b.put(ctx, 0, key, value)
}

func (b *Builder) put(ctx context.Context, level int, key, value []byte) error {
	b.ctx = ctx
	defer func() { b.ctx = nil }()
	if b.isDone {
		return errors.Errorf("builder is closed")
	}
	if b.syncLevel() < level {
		return errors.Errorf("cannot put at level %d", level)
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

func (b *Builder) syncLevel() int {
	for i := range b.levels {
		if b.levels[i].Buffered() > 0 {
			return i
		}
	}
	return MaxTreeDepth - 1 // allow copying at any depth
}
