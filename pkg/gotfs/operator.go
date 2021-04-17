package gotfs

import (
	"context"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/gdat"
	"github.com/brendoncarroll/got/pkg/gotkv"
)

type (
	Ref   = gotkv.Ref
	Store = gotkv.Store
	Root  = gotkv.Root
)

type Option func(o *Operator)

func WithDataOperator(dop *gdat.Operator) Option {
	return func(o *Operator) {
		o.dop = dop
	}
}

type Operator struct {
	dop   *gdat.Operator
	gotkv gotkv.Operator
}

func NewOperator(opts ...Option) *Operator {
	o := &Operator{
		dop: gdat.NewOperator(),
	}
	for _, opt := range opts {
		opt(o)
	}
	o.gotkv = gotkv.NewOperator(gotkv.WithRefOperator(o.dop))
	return o
}

var defaultOp = NewOperator()

// Select returns a new root containing everything under p, shifted to the root.
func (o *Operator) Select(ctx context.Context, s cadata.Store, root Root, p string) (*Root, error) {
	_, err := o.GetMetadata(ctx, s, root, p)
	if err != nil {
		return nil, err
	}
	x := &root
	if x, err = o.deleteOutside(ctx, s, *x, gotkv.PrefixSpan([]byte(p))); err != nil {
		return nil, err
	}
	if x, err = o.gotkv.RemovePrefix(ctx, s, *x, []byte(p)); err != nil {
		return nil, err
	}
	return x, err
}

func (o *Operator) deleteOutside(ctx context.Context, s cadata.Store, root Root, span gotkv.Span) (*Root, error) {
	x := &root
	var err error
	if x, err = o.gotkv.DeleteSpan(ctx, s, *x, gotkv.Span{Start: nil, End: span.Start}); err != nil {
		return nil, err
	}
	if x, err = o.gotkv.DeleteSpan(ctx, s, *x, gotkv.Span{Start: span.End, End: nil}); err != nil {
		return nil, err
	}
	return x, err
}
