package maybe

import "fmt"

type Maybe[T any] struct {
	Ok bool
	X  T
}

func (m Maybe[T]) String() string {
	if !m.Ok {
		return "Nothing"
	} else {
		return fmt.Sprintf("Just(%v)", m.X)
	}
}

func (m Maybe[T]) Clone(fn func(dst *T, src T)) Maybe[T] {
	if fn == nil {
		fn = func(dst *T, src T) { *dst = src }
	}
	if !m.Ok {
		return Nothing[T]()
	}
	var x T
	fn(&x, m.X)
	return Just(x)
}

func Just[T any](x T) Maybe[T] {
	return Maybe[T]{Ok: true, X: x}
}

func Nothing[T any]() Maybe[T] {
	return Maybe[T]{}
}

func Map[A, B any](x Maybe[A], fn func(A) B) Maybe[B] {
	if !x.Ok {
		return Nothing[B]()
	}
	return Just(fn(x.X))
}

func FlatMap[A, B any](x Maybe[A], fn func(A) Maybe[B]) Maybe[B] {
	if !x.Ok {
		return Nothing[B]()
	}
	return fn(x.X)
}

func Copy[T any](dst *Maybe[T], src Maybe[T], copy func(dst *T, src T)) {
	if !src.Ok {
		return
	}
	dst.Ok = true
	copy(&dst.X, src.X)
}