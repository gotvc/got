package gotrepo

import (
	"context"
	"sync/atomic"

	"golang.org/x/sync/semaphore"
)

type lazySetup[T any] struct {
	isSetup atomic.Bool
	sem     semaphore.Weighted
	x       T
	fn      func(ctx context.Context) (T, error)
}

func newLazySetup[T any](fn func(ctx context.Context) (T, error)) lazySetup[T] {
	return lazySetup[T]{
		sem: *semaphore.NewWeighted(1),
		fn:  fn,
	}
}

func (ls *lazySetup[T]) Use(ctx context.Context) (T, error) {
	isSetup := ls.isSetup.Load()
	if isSetup {
		return ls.x, nil
	}

	if err := ls.sem.Acquire(ctx, 1); err != nil {
		var zero T
		return zero, err
	}
	defer ls.sem.Release(1)
	if isSetup {
		return ls.x, nil
	}
	x, err := ls.fn(ctx)
	if err != nil {
		var zero T
		return zero, err
	}
	ls.x = x
	ls.isSetup.Store(true)
	return ls.x, nil
}
