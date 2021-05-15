package gotkv

import (
	"context"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/gdat"
	"github.com/brendoncarroll/got/pkg/ptree"
	"github.com/pkg/errors"
)

type (
	Store = cadata.Store
	ID    = cadata.ID
	Ref   = gdat.Ref

	Entry = ptree.Entry
	Root  = ptree.Root
	Span  = ptree.Span
)

var ErrKeyNotFound = errors.Errorf("key not found")

var defaultOperator = NewOperator()

func Get(ctx context.Context, s Store, x Root, key []byte) ([]byte, error) {
	return defaultOperator.Get(ctx, s, x, key)
}

func GetF(ctx context.Context, s Store, x Root, key []byte, fn func([]byte) error) error {
	return defaultOperator.GetF(ctx, s, x, key, fn)
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
		for _, ent := range ents {
			if err := entryFn(ent); err != nil {
				return err
			}
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
func CopyAll(ctx context.Context, b Builder, it Iterator) error {
	ptb, ok1 := b.(*ptree.Builder)
	pti, ok2 := it.(*ptree.Iterator)
	if !ok1 || !ok2 {
		return errors.Errorf("CopyAll not supported on b=%T it=%T", b, it)
	}
	return ptree.CopyAll(ctx, ptb, pti)
}

// Populate adds all blobs reachable from x to set.
// If an item is in set all of the blobs reachable from it are also assumed to also be in set.
func Populate(ctx context.Context, s Store, x Root, set cadata.Set, entryFn func(ent Entry) error) error {
	op := gdat.NewOperator()
	if exists, err := set.Exists(ctx, x.Ref.CID); err != nil {
		return err
	} else if exists {
		return nil
	}
	if x.Depth == 0 {
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
