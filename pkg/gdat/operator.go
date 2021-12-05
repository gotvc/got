package gdat

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	lru "github.com/hashicorp/golang-lru"
)

type Store = cadata.Store

type Option = func(*Operator)

func WithSalt(salt *[32]byte) Option {
	if salt == nil {
		panic("gdat.WithSalt called with nil")
	}
	return func(o *Operator) {
		o.kf = SaltedConvergent(salt)
	}
}

func WithCacheSize(n int) Option {
	return func(o *Operator) {
		o.cacheSize = n
	}
}

type Operator struct {
	kf KeyFunc

	cacheSize int
	cache     *lru.ARCCache
}

func NewOperator(opts ...Option) Operator {
	o := Operator{
		kf:        Convergent,
		cacheSize: 16,
	}
	for _, opt := range opts {
		opt(&o)
	}
	var err error
	if o.cache, err = lru.NewARC(o.cacheSize); err != nil {
		panic(err)
	}
	return o
}

func (o *Operator) Post(ctx context.Context, s Store, data []byte) (*Ref, error) {
	id, dek, err := postEncrypt(ctx, s, o.kf, data)
	if err != nil {
		return nil, err
	}
	return &Ref{
		CID: id,
		DEK: *dek,
	}, nil
}

func (o *Operator) GetF(ctx context.Context, s Store, ref Ref, fn func(data []byte) error) error {
	if data := o.checkCache(ref); data != nil {
		return fn(data)
	}
	buf := make([]byte, s.MaxSize())
	n, err := o.Read(ctx, s, ref, buf)
	if err != nil {
		return err
	}
	data := buf[:n]
	o.loadCache(ref, data)
	return fn(data)
}

func (o *Operator) Read(ctx context.Context, s Store, ref Ref, buf []byte) (int, error) {
	return getDecrypt(ctx, s, ref.DEK, ref.CID, buf)
}

func (o *Operator) checkCache(ref Ref) []byte {
	data, exists := o.cache.Get(ref)
	if !exists {
		return nil
	}
	return data.([]byte)
}

func (o *Operator) loadCache(ref Ref, data []byte) {
	o.cache.Add(ref, append([]byte{}, data...))
}
