package gotvc

import (
	"github.com/gotvc/got/pkg/gdat"
)

type Option = func(a *Agent)

func WithSalt(salt *[32]byte) Option {
	return func(a *Agent) {
		a.salt = salt
	}
}

type Agent struct {
	salt      *[32]byte
	cacheSize int
	readOnly  bool
	da        *gdat.Agent
}

func NewAgent(opts ...Option) *Agent {
	ag := Agent{
		cacheSize: 256,
		salt:      &[32]byte{},
	}
	for _, opt := range opts {
		opt(&ag)
	}
	ag.da = gdat.NewAgent(gdat.WithSalt(ag.salt), gdat.WithCacheSize(ag.cacheSize))
	return &ag
}
