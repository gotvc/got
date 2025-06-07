package gotrope

import (
	"context"
	"fmt"
)

type Walker[Ref any] struct {
	// If Before returns false, the descendents are skipped.
	Before func(Ref) bool
	// ForEach is called for each entry
	ForEach func(Entry) error
	// After is called on a node after it's descendants have been completed.
	After func(Ref) error
}

func Walk[Ref any](ctx context.Context, s Storage[Ref], root Root[Ref], w Walker[Ref]) error {
	return walk(ctx, s, w, root.Ref, root.Depth, Weight{0})
}

func walk[Ref any](ctx context.Context, s Storage[Ref], w Walker[Ref], ref Ref, depth uint8, offset Weight) error {
	if depth > 0 {
		if !w.Before(ref) {
			return nil
		}
		idxs, err := ListIndexes(ctx, s, ref)
		if err != nil {
			return err
		}
		if len(idxs) == 0 {
			return fmt.Errorf("index node without entries")
		}
		offsets := make([]Weight, len(idxs))
		offsets[0] = offset
		for i, idx := range idxs {
			if i < len(idxs)-1 {
				offsets[i+1].Add(offsets[i], idx.Weight)
			}
			if err := walk(ctx, s, w, idx.Ref, depth-1, offsets[i]); err != nil {
				return err
			}
		}
	} else {
		ents, err := ListEntries(ctx, s, offset, ref)
		if err != nil {
			return err
		}
		for _, ent := range ents {
			if err := w.ForEach(ent); err != nil {
				return err
			}
		}
	}
	return w.After(ref)
}
