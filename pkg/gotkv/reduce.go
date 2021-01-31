package gotkv

import (
	"context"

	"golang.org/x/sync/errgroup"
)

type ReduceFunc = func(key []byte, lv, rv []byte) ([]byte, error)

// Reduce performs a key wise reduction on xs.
// ReduceFunc is assumed to be associative, and non-commutative
// If the same key exists in two xs, then ReduceFunc is called to get the final value for that key
// Keys that only exist in one will have the value copied to the output
func Reduce(ctx context.Context, s Store, xs []Ref, fn ReduceFunc) (*Ref, error) {
	switch len(xs) {
	case 0:
		return New(ctx, s)
	case 1:
		return &xs[0], nil
	case 2:
		return reduce2(ctx, s, xs[0], xs[1], fn)
	default:
		l := len(xs)
		eg := errgroup.Group{}
		var left, right *Ref
		eg.Go(func() error {
			y, err := Reduce(ctx, s, xs[:l/2], fn)
			left = y
			return err
		})
		eg.Go(func() error {
			y, err := Reduce(ctx, s, xs[l/2:], fn)
			right = y
			return err
		})
		if err := eg.Wait(); err != nil {
			return nil, err
		}
		return Reduce(ctx, s, []Ref{*left, *right}, fn)
	}
}

func reduce2(ctx context.Context, s Store, left, right Ref, fn ReduceFunc) (*Ref, error) {
	leftIter := NewIterator(ctx, s, left)
	rightIter := NewIterator(ctx, s, right)
	panic(leftIter)
	panic(rightIter)
}

var _ ReduceFunc = Concat

// Concat is a Reducer which concatenates values
func Concat(k, l, r []byte) ([]byte, error) {
	x := make([]byte, 0, len(l)+len(r))
	x = append(x, l...)
	x = append(x, r...)
	return x, nil
}

// TakeRight is a Reducer which always takes the right value.
func TakeRight(k, l, r []byte) ([]byte, error) {
	return r, nil
}

// TakeLeft is a Reducer which always takes the left value.
func TakeLeft(k, l, r []byte) ([]byte, error) {
	return l, nil
}
