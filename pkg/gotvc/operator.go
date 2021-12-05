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
		seed:      &[32]byte{},
	}
	for _, opt := range opts {
		opt(&op)
	}
	op.dop = gdat.NewOperator(gdat.WithSalt(op.seed), gdat.WithCacheSize(op.cacheSize))
	return op
}
