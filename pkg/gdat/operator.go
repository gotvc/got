package gdat

import (
	"context"
	"sync"

	lru "github.com/hashicorp/golang-lru"
	"go.brendoncarroll.net/state/cadata"
)

type (
	Getter = cadata.Getter
	Store  = cadata.Store
)

type Option = func(*Agent)

func WithSalt(salt *[32]byte) Option {
	if salt == nil {
		panic("gdat.WithSalt called with nil")
	}
	return func(a *Agent) {
		a.kf = SaltedConvergent(salt)
	}
}

func WithCacheSize(n int) Option {
	return func(a *Agent) {
		a.cacheSize = n
	}
}

type Agent struct {
	kf KeyFunc

	cacheSize int
	cache     *lru.Cache
	pool      sync.Pool
}

func NewAgent(opts ...Option) *Agent {
	o := &Agent{
		kf:        Convergent,
		cacheSize: 32,
	}
	for _, opt := range opts {
		opt(o)
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

func (a *Agent) Post(ctx context.Context, s cadata.Poster, data []byte) (*Ref, error) {
	id, dek, err := a.postEncrypt(ctx, s, a.kf, data)
	if err != nil {
		return nil, err
	}
	return &Ref{
		CID: id,
		DEK: *dek,
	}, nil
}

func (a *Agent) GetF(ctx context.Context, s Getter, ref Ref, fn func(data []byte) error) error {
	if data := a.checkCache(ref); data != nil {
		return fn(data)
	}
	buf := a.acquire(s.MaxSize())
	n, err := a.Read(ctx, s, ref, buf)
	if err != nil {
		return err
	}
	data := buf[:n]
	a.loadCache(ref, data)
	return fn(data)
}

func (a *Agent) Read(ctx context.Context, s Getter, ref Ref, buf []byte) (int, error) {
	return getDecrypt(ctx, s, ref.DEK, ref.CID, buf)
}

func (a *Agent) checkCache(ref Ref) []byte {
	data, exists := a.cache.Get(ref)
	if !exists {
		return nil
	}
	return data.([]byte)
}

func (a *Agent) loadCache(ref Ref, data []byte) {
	a.cache.Add(ref, append([]byte{}, data...))
}

func (a *Agent) onCacheEvict(key, value any) {
	buf := value.([]byte)
	a.release(buf)
}

func (a *Agent) acquire(n int) []byte {
	buf := a.pool.Get().([]byte)
	if len(buf) < n {
		buf = make([]byte, n)
	}
	return buf
}

func (a *Agent) release(x []byte) {
	x = append(x, make([]byte, cap(x)-len(x))...)
	a.pool.Put(x)
}
