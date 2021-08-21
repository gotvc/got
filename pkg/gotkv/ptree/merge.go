package ptree

import (
	"context"
	"io"
)

// Merge performs a key-wise merge on the tree
func Merge(ctx context.Context, b *Builder, roots []Root) (*Root, error) {
	if err := merge(ctx, b, roots); err != nil {
		return nil, err
	}
	return b.Finish(ctx)
}

// TODO: this can be more efficient, right now it does a naive O(n) merge.
func merge(ctx context.Context, b *Builder, roots []Root) error {
	streams := make([]StreamIterator, len(roots))
	for i := range roots {
		streams[i] = NewIterator(b.s, b.op, roots[i], TotalSpan())
	}
	sm := NewStreamMerger(b.s, streams)
	for {
		ent, err := sm.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if err := b.Put(ctx, ent.Key, ent.Value); err != nil {
			return err
		}
	}
	return nil
}
