package ptree

import (
	"context"

	"github.com/gotvc/got/pkg/gotkv/kv"
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
	streams := make([]kv.Iterator, len(roots))
	for i := range roots {
		streams[i] = NewIterator(b.s, b.op, roots[i], kv.TotalSpan())
	}
	sm := NewStreamMerger(b.s, streams)
	var ent Entry
	for {
		if err := sm.Next(ctx, &ent); err != nil {
			if err == kv.EOS {
				return nil
			}
			return err
		}
		if err := b.Put(ctx, ent.Key, ent.Value); err != nil {
			return err
		}
	}
}
