package gotkv

import (
	"context"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/gotkv/gkvproto"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

func forEachRoot(ctx context.Context, s cadata.Store, numThreads int, ref Ref, fn func(k, v []byte) error) error {
	sem := semaphore.NewWeighted(int64(numThreads))
	return forEach(ctx, s, sem, ref, fn)
}

func forEach(ctx context.Context, s cadata.Store, sem *semaphore.Weighted, ref Ref, fn func(k, v []byte) error) error {
	return getNodeF(ctx, s, ref, func(n Node) error {
		if err := sem.Acquire(ctx, 1); err != nil {
			return err
		}
		return nodeForEach(ctx, s, sem, n, fn)
	})
}

func nodeForEach(ctx context.Context, s cadata.Store, sem *semaphore.Weighted, n Node, fn func(k, v []byte) error) error {
	switch n.Which() {
	case gkvproto.Node_Which_leaf:
		leaf, err := n.Leaf()
		if err != nil {
			return err
		}
		ents, err := leaf.Entries()
		if err != nil {
			return err
		}
		return forEachEntries(ents, fn)
	case gkvproto.Node_Which_tree:
		tree, err := n.Tree()
		if err != nil {
			return err
		}
		return treeForEach(ctx, s, sem, tree, fn)

	default:
		return errInvalidNode()
	}
}

func forEachEntries(ents gkvproto.Entry_List, fn func(k, v []byte) error) error {
	for i := 0; i < ents.Len(); i++ {
		ent := ents.At(i)
		k, err := ent.Key()
		if err != nil {
			return err
		}
		v, err := ent.Value()
		if err != nil {
			return err
		}
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

func treeForEach(ctx context.Context, s Store, sem *semaphore.Weighted, tree gkvproto.Tree, fn func(k, v []byte) error) error {
	children, err := tree.Children()
	if err != nil {
		return err
	}
	eg := errgroup.Group{}
	for i := 0; i < children.Len(); i++ {
		childRef := children.At(i)
		ref, err := childRef.Ref()
		if err != nil {
			return err
		}
		prefix, err := childRef.Prefix()
		if err != nil {
			return err
		}
		nextFn := func(k, v []byte) error {
			k = append(append([]byte{}, prefix...), k...)
			return fn(k, v)
		}
		if sem.TryAcquire(1) {
			eg.Go(func() error {
				defer sem.Release(1)
				return forEach(ctx, s, sem, ref, nextFn)
			})
		} else {
			if err := forEach(ctx, s, sem, ref, nextFn); err != nil {
				return err
			}
			continue
		}
	}
	return eg.Wait()
}
