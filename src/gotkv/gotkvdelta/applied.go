package gotkvdelta

import (
	"context"

	"github.com/gotvc/got/src/gotkv"
	"go.brendoncarroll.net/exp/streams"
)

// Applied represents a sequence of Deltas applied to a Base gotkv.Root
type Applied struct {
	Base   gotkv.Root
	Deltas []Delta
}

func (a *Applied) Iterate(span Span) Iterator {
	return Iterator{a: a}
}

var _ streams.Iterator[gotkv.Entry] = &Iterator{}

type Iterator struct {
	a       *Applied
	span    Span
	lastKey []byte
}

func (it *Iterator) Next(ctx context.Context, dst []gotkv.Entry) (int, error) {
	return 0, nil
}
