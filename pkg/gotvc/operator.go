package gotvc

import (
	"github.com/gotvc/got/pkg/gdat"
)

type Option = func(o *Operator)

func WithDataOperator(dop gdat.Operator) Option {
	return func(o *Operator) {
		o.dop = dop
	}
}

type Operator struct {
	dop      gdat.Operator
	readOnly bool
}

func NewOperator(opts ...Option) Operator {
	op := Operator{
		dop: gdat.NewOperator(),
	}
	for _, opt := range opts {
		opt(&op)
	}
	return op
}
