package gotvc

import (
	"github.com/gotvc/got/pkg/gdat"
)

type Option = func(a *Machine)

func WithSalt(salt *[32]byte) Option {
	return func(a *Machine) {
		a.salt = salt
	}
}

type Machine struct {
	salt      *[32]byte
	cacheSize int
	readOnly  bool
	da        *gdat.Machine
}

func NewMachine(opts ...Option) *Machine {
	ag := Machine{
		cacheSize: 256,
		salt:      &[32]byte{},
	}
	for _, opt := range opts {
		opt(&ag)
	}
	ag.da = gdat.NewMachine(gdat.WithSalt(ag.salt), gdat.WithCacheSize(ag.cacheSize))
	return &ag
}
