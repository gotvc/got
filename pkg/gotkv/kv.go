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

func Copy(ctx context.Context, dst, src Store, x Root, entryFn func(Entry) error) error {
	if x.Depth <= 1 {
		ents, err := ptree.ListEntries(ctx, src, ptree.Index{First: x.First, Ref: x.Ref})
		if err != nil {
			return err
		}
		for _, ent := range ents {
			if err := entryFn(ent); err != nil {
				return err
			}
		}
	} else {
		idxs, err := ptree.ListChildren(ctx, src, x)
		if err != nil {
			return err
		}
		for _, idx := range idxs {
			if err := Copy(ctx, dst, src, Root{Ref: idx.Ref, First: idx.First, Depth: x.Depth - 1}, entryFn); err != nil {
				return err
			}
		}
	}
	return cadata.Copy(ctx, dst, src, x.Ref.CID)
}
