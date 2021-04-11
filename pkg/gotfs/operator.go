package gotfs

import (
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
