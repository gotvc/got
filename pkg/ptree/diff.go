package ptree

import (
	"bytes"
	"context"
	"io"

	"github.com/brendoncarroll/got/pkg/cadata"
)

type DiffFn = func(key, leftValue, rightValue []byte) error

// Diff calls fn with all the keys and values that are different between the two trees.
func Diff(ctx context.Context, s cadata.Store, left, right Root, span Span, fn DiffFn) error {
	leftIt := NewIterator(s, left, span)
	rightIt := NewIterator(s, right, span)

	var leftEnt, rightEnt *Entry
	emitLeft := func() {
		fn(leftEnt.Key, leftEnt.Value, nil)
		leftEnt = nil
	}
	emitRight := func() {
		fn(rightEnt.Key, nil, rightEnt.Value)
		rightEnt = nil
	}
	for {
		if leftEnt == nil {
			var err error
			leftEnt, err = leftIt.Next(ctx)
			if err != nil && err != io.EOF {
				return err
			}
		}
		if rightEnt == nil {
			var err error
			rightEnt, err = rightIt.Next(ctx)
			if err != nil && err != io.EOF {
				return err
			}
		}
		switch {
		case leftEnt == nil && rightEnt == nil:
			return nil
		case leftEnt != nil && rightEnt == nil:
			emitLeft()
		case leftEnt == nil && rightEnt != nil:
			emitRight()
		default:
			cmp := bytes.Compare(leftEnt.Key, rightEnt.Key)
			if cmp == 0 {
				if !bytes.Equal(leftEnt.Value, rightEnt.Value) {
					fn(leftEnt.Key, leftEnt.Value, rightEnt.Value)
				}
				leftEnt, rightEnt = nil, nil
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
