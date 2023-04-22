package gotvc

import (
	"github.com/gotvc/got/pkg/gdat"
)

type Option = func(o *Operator)

func WithSalt(salt *[32]byte) Option {
	return func(o *Operator) {
		o.salt = salt
	}
}

type Operator struct {
	salt      *[32]byte
	cacheSize int
	readOnly  bool
	dop       *gdat.Operator
}

func NewOperator(opts ...Option) *Operator {
	op := Operator{
		cacheSize: 256,
		salt:      &[32]byte{},
	}
	for _, opt := range opts {
		opt(&op)
	}
	op.dop = gdat.NewOperator(gdat.WithSalt(op.salt), gdat.WithCacheSize(op.cacheSize))
	return &op
}
