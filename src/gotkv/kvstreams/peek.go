package kvstreams

import (
	"context"

	"go.brendoncarroll.net/exp/maybe"
	"go.brendoncarroll.net/exp/streams"
)

type Peeker[T any] struct {
	x  streams.Iterator[T]
	cp func(dst *T, src T)

	next maybe.Maybe[T]
}

func NewPeeker[T any](x streams.Iterator[T], cp func(dst *T, src T)) streams.Peekable[T] {
	if p, ok := x.(streams.Peekable[T]); ok {
		return p
	}
	return &Peeker[T]{
		x:  x,
		cp: cp,
	}
}

func (pi *Peeker[T]) Next(ctx context.Context, dst *T) error {
	if pi.next.Ok {
		pi.cp(dst, pi.next.X)
		pi.next.Ok = false
		return nil
	}
	return pi.x.Next(ctx, dst)
}

func (pi *Peeker[T]) Peek(ctx context.Context, dst *T) error {
	if !pi.next.Ok {
		if err := pi.x.Next(ctx, &pi.next.X); err != nil {
			return err
		}
		pi.next.Ok = true
	}
	pi.cp(dst, pi.next.X)
	return nil
}
