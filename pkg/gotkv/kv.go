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
