package sqlutil

import (
	"context"
	"iter"

	"go.brendoncarroll.net/exp/streams"
)

type Iterator[T any] struct {
	next func() (T, error, bool)
	stop func()
}

func NewIterator[T any](x iter.Seq2[T, error]) Iterator[T] {
	next, stop := iter.Pull2(x)
	return Iterator[T]{
		next: next,
		stop: stop,
	}
}

func (it *Iterator[T]) Next(ctx context.Context, dst []T) (int, error) {
	for i := range dst {
		y, err, ok := it.next()
		if !ok {
			if i > 0 {
				return i, nil
			} else {
				return 0, streams.EOS()
			}
		}
		if err != nil {
			return 0, err
		}
		dst[i] = y
	}
	return len(dst), nil
}

func (it *Iterator[T]) Drop() {
	it.stop()
}

func (it *Iterator[T]) Close() error {
	it.Drop()
	return nil
}
