package gotvc

import (
	"github.com/gotvc/got/pkg/gdat"
)

type Option = func(o *Operator)

func WithSeed(seed *[32]byte) Option {
	return func(o *Operator) {
		o.seed = seed
	}
}

type Operator struct {
	seed      *[32]byte
	cacheSize int
	readOnly  bool
	dop       gdat.Operator
}

func NewOperator(opts ...Option) Operator {
	op := Operator{
		cacheSize: 256,
	}
	for _, opt := range opts {
		opt(&op)
	}
	var gdatOpts []gdat.Option
	gdatOpts = append(gdatOpts, gdat.WithCacheSize(op.cacheSize))
	if op.seed != nil {
		gdatOpts = append(gdatOpts, gdat.WithSalt(op.seed))
	}
	op.dop = gdat.NewOperator(gdatOpts...)
	return op
}
