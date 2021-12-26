package gotkv

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
	"github.com/gotvc/got/pkg/gotkv/ptree"
	"github.com/gotvc/got/pkg/stores"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type (
	Store = cadata.Store
	ID    = cadata.ID
	Ref   = gdat.Ref

	Entry = kvstreams.Entry
	Span  = kvstreams.Span

	Root = ptree.Root
)

var (
	ErrKeyNotFound = errors.Errorf("key not found")
	EOS            = kvstreams.EOS
)

var defaultReadOnlyOperator = &Operator{dop: gdat.NewOperator()}

// Get is a convenience function for performing Get without creating an Operator.
func Get(ctx context.Context, s Store, x Root, key []byte) ([]byte, error) {
	return defaultReadOnlyOperator.Get(ctx, s, x, key)
}

// GetF is a convenience function for performing GetF without creating an Operator
func GetF(ctx context.Context, s Store, x Root, key []byte, fn func([]byte) error) error {
	return defaultReadOnlyOperator.GetF(ctx, s, x, key, fn)
}

// Sync ensures dst has all the data reachable from x.
func Sync(ctx context.Context, dst, src Store, x Root, entryFn func(Entry) error) error {
	if exists, err := dst.Exists(ctx, x.Ref.CID); err != nil {
		return err
	} else if exists {
		return nil
	}
	op := gdat.NewOperator()
	if x.Depth == 0 {
		ents, err := ptree.ListEntries(ctx, src, &op, ptree.Index{First: x.First, Ref: x.Ref})
		if err != nil {
			return err
		}
		eg := errgroup.Group{}
		for _, ent := range ents {
			ent := ent
			eg.Go(func() error {
				return entryFn(ent)
			})
		}
		if err := eg.Wait(); err != nil {
			return err
		}
	} else {
		idxs, err := ptree.ListChildren(ctx, src, &op, x)
		if err != nil {
			return err
		}
		for _, idx := range idxs {
			root2 := Root{
				Ref:   idx.Ref,
				First: idx.First,
				Depth: x.Depth - 1,
			}
			if err := Sync(ctx, dst, src, root2, entryFn); err != nil {
				return err
			}
		}
	}
	return cadata.Copy(ctx, dst, src, x.Ref.CID)
}

// CopyAll copies all the entries from iterator to builder.
func CopyAll(ctx context.Context, b *Builder, it Iterator) error {
	if pti, ok := it.(*ptree.Iterator); ok {
		return ptree.Copy(ctx, b, pti)
	}
	return kvstreams.ForEach(ctx, it, func(ent Entry) error {
		return b.Put(ctx, ent.Key, ent.Value)
	})
}

// Populate adds all blobs reachable from x to set.
// If an item is in set all of the blobs reachable from it are also assumed to also be in set.
func Populate(ctx context.Context, s Store, x Root, set stores.Set, entryFn func(ent Entry) error) error {
	op := gdat.NewOperator()
	if exists, err := set.Exists(ctx, x.Ref.CID); err != nil {
		return err
	} else if exists {
		return nil
	}
	if ptree.PointsToEntries(x) {
		ents, err := ptree.ListEntries(ctx, s, &op, ptree.Index{First: x.First, Ref: x.Ref})
		if err != nil {
			return err
		}
		for _, ent := range ents {
			if err := entryFn(ent); err != nil {
				return err
			}
		}
	} else {
		idxs, err := ptree.ListChildren(ctx, s, &op, x)
		if err != nil {
			return err
		}
		for _, idx := range idxs {
			root2 := Root{
				Ref:   idx.Ref,
				First: idx.First,
				Depth: x.Depth - 1,
			}
			if err := Populate(ctx, s, root2, set, entryFn); err != nil {
				return err
			}
		}
	}
	return set.Add(ctx, x.Ref.CID)
}
