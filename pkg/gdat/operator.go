package gdat

import (
	"context"
	"sync"

	"github.com/brendoncarroll/go-state/cadata"
	lru "github.com/hashicorp/golang-lru"
)

type (
	Getter = cadata.Getter
	Store  = cadata.Store
)

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
	cache     *lru.Cache
	pool      sync.Pool
}

func NewOperator(opts ...Option) Operator {
	o := Operator{
		kf:        Convergent,
		cacheSize: 32,
	}
	for _, opt := range opts {
		opt(&o)
	}
	var err error
	if o.cache, err = lru.NewWithEvict(o.cacheSize, o.onCacheEvict); err != nil {
		panic(err)
	}
	o.pool.New = func() any {
		return []byte{}
	}
	return o
}

func (o *Operator) Post(ctx context.Context, s cadata.Poster, data []byte) (*Ref, error) {
	id, dek, err := o.postEncrypt(ctx, s, o.kf, data)
	if err != nil {
		return nil, err
	}
	return &Ref{
		CID: id,
		DEK: *dek,
	}, nil
}

func (o *Operator) GetF(ctx context.Context, s Getter, ref Ref, fn func(data []byte) error) error {
	if data := o.checkCache(ref); data != nil {
		return fn(data)
	}
	buf := o.acquire(s.MaxSize())
	n, err := o.Read(ctx, s, ref, buf)
	if err != nil {
		return err
	}
	data := buf[:n]
	o.loadCache(ref, data)
	return fn(data)
}

func (o *Operator) Read(ctx context.Context, s Getter, ref Ref, buf []byte) (int, error) {
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

func (o *Operator) onCacheEvict(key, value any) {
	buf := value.([]byte)
	o.release(buf)
}

func (o *Operator) acquire(n int) []byte {
	buf := o.pool.Get().([]byte)
	if len(buf) < n {
		buf = make([]byte, n)
	}
	return buf
}

func (o *Operator) release(x []byte) {
	x = append(x, make([]byte, cap(x)-len(x))...)
	o.pool.Put(x)
}
