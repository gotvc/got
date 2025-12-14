package kvstreams

import (
	"context"

	"go.brendoncarroll.net/exp/streams"
)

type Map[X, Y any] struct {
	xs streams.Iterator[X]
	fn func(y *Y, x X)
	x  X
}

func NewMap[X, Y any](xs streams.Iterator[X], fn func(y *Y, x X)) *Map[X, Y] {
	return &Map[X, Y]{
		xs: xs,
		fn: fn,
	}
}

func (m Map[X, Y]) Next(ctx context.Context, dst *Y) error {
	if err := m.xs.Next(ctx, &m.x); err != nil {
		return err
	}
	m.fn(dst, m.x)
	return nil
}
