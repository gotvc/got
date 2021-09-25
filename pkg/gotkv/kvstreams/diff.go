package kvstreams

import (
	"bytes"
	"context"

	"github.com/brendoncarroll/go-state/cadata"
)

type DiffFn = func(key, leftValue, rightValue []byte) error

// Diff calls fn with all the keys and values that are different between the two Iterators
func Diff(ctx context.Context, s cadata.Store, leftIt, rightIt Iterator, span Span, fn DiffFn) error {
	var leftExists, rightExists bool
	var leftEnt, rightEnt Entry
	emitLeft := func() {
		fn(leftEnt.Key, leftEnt.Value, nil)
		leftExists = false
	}
	emitRight := func() {
		fn(rightEnt.Key, nil, rightEnt.Value)
		rightExists = false
	}
	for {
		if !leftExists {
			if err := leftIt.Next(ctx, &leftEnt); err != nil && err != EOS {
				return err
			}
			leftExists = true
		}
		if !rightExists {
			if err := rightIt.Next(ctx, &rightEnt); err != nil && err != EOS {
				return err
			}
			rightExists = true
		}
		switch {
		case !leftExists && !rightExists:
			return nil
		case leftExists && !rightExists:
			emitLeft()
		case !leftExists && rightExists:
			emitRight()
		default:
			cmp := bytes.Compare(leftEnt.Key, rightEnt.Key)
			if cmp == 0 {
				if !bytes.Equal(leftEnt.Value, rightEnt.Value) {
					fn(leftEnt.Key, leftEnt.Value, rightEnt.Value)
				}
				leftExists, rightExists = false, false
			} else if cmp < 0 {
				emitLeft()
			} else if cmp > 0 {
				emitRight()
			} else {
				panic("unreachable")
			}
		}
	}
}
