package kvstreams

import (
	"context"

	"go.brendoncarroll.net/exp/maybe"
	"go.brendoncarroll.net/exp/streams"
)

var _ Iterator = &Merger[Entry]{}

type Merger[T any] struct {
	inputs []streams.Peekable[T]
	cmp    func(a, b T) int
}

// NewMerger creates a new merging stream and returns it.
// cmp is used to determine which element should be emitted next.
func NewMerger[T any](inputs []streams.Peekable[T], cmp func(a, b T) int) *Merger[T] {
	return &Merger[T]{
		inputs: inputs,
		cmp:    cmp,
	}
}

func (sm *Merger[T]) Next(ctx context.Context, dst *T) error {
	sr, err := sm.selectStream(ctx)
	if err != nil {
		return err
	}
	if err := sr.Next(ctx, dst); err != nil {
		return err
	}
	return sm.advancePast(ctx, *dst)
}

func (sm *Merger[T]) Peek(ctx context.Context, dst *T) error {
	sr, err := sm.selectStream(ctx)
	if err != nil {
		return err
	}
	return sr.Peek(ctx, dst)
}

func (sm *Merger[T]) advancePast(ctx context.Context, k T) error {
	var x T
	for _, sr := range sm.inputs {
		if err := sr.Peek(ctx, &x); err != nil {
			if streams.IsEOS(err) {
				continue
			}
			return err
		}
		// if the stream is behind, advance it.
		if sm.cmp(x, k) <= 0 {
			if err := sr.Next(ctx, &x); err != nil {
				return err
			}
		}
	}
	return nil
}

// selectStream will never return an ended stream
func (sm *Merger[T]) selectStream(ctx context.Context) (streams.Peekable[T], error) {
	var minTMaybe maybe.Maybe[T]
	nextIndex := len(sm.inputs)
	var ent T
	for i, sr := range sm.inputs {
		if err := sr.Peek(ctx, &ent); err != nil {
			if streams.IsEOS(err) {
				continue
			}
			return nil, err
		}
		if !minTMaybe.Ok || sm.cmp(ent, minTMaybe.X) <= 0 {
			minTMaybe = maybe.Just(ent)
			nextIndex = i
		}
	}
	if nextIndex < len(sm.inputs) {
		return sm.inputs[nextIndex], nil
	}
	return nil, streams.EOS()
}
