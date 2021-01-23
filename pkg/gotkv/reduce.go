package gotkv

import "context"

type ReduceFunc func(key []byte, v1, v2 []byte) ([]byte, error)

// Reduce performs a key wise reduction on xs.
// ReduceFunc is assumed to be non-commutative
// If the same key exists in two xs, then ReduceFunc is called to get the final value for that key
func Reduce(ctx context.Context, s Store, xs []Ref, fn ReduceFunc) (*Ref, error) {
	panic("")
}
