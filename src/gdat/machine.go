package gdat

import (
	"context"
	"sync"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/internal/stores"
	lru "github.com/hashicorp/golang-lru"
)

type Params struct {
	Salt          [32]byte
	CacheSize     *int
	KeyedHashFunc blobcache.KeyedHashFunc
}

func (p Params) GetKeyedHashFunc() blobcache.KeyedHashFunc {
	if p.KeyedHashFunc == nil {
		return stores.HashAlgo.KeyedHash
	}
	return p.KeyedHashFunc
}

func (p Params) GetCacheSize() int {
	if p.CacheSize == nil {
		return 32
	}
	return *p.CacheSize
}

type Machine struct {
	salt blobcache.CID
	khf  blobcache.KeyedHashFunc

	cacheSize int
	cache     *lru.Cache
	pool      sync.Pool
}

func NewMachine(p Params) *Machine {
	o := &Machine{
		salt: p.Salt,
		khf:  p.GetKeyedHashFunc(),

		cacheSize: 32,
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

func (m *Machine) Post(ctx context.Context, s stores.WO, data []byte) (Ref, error) {
	id, dek, err := m.postEncrypt(ctx, s, data)
	if err != nil {
		return Ref{}, err
	}
	return Ref{
		CID: id,
		DEK: *dek,
	}, nil
}

func (m *Machine) GetF(ctx context.Context, s stores.RO, ref Ref, fn func(data []byte) error) error {
	if data := m.checkCache(ref); data != nil {
		return fn(data)
	}
	buf := m.acquire(s.MaxSize())
	n, err := m.Read(ctx, s, ref, buf)
	if err != nil {
		return err
	}
	data := buf[:n]
	m.loadCache(ref, data)
	return fn(data)
}

func (m *Machine) Read(ctx context.Context, s stores.RO, ref Ref, buf []byte) (int, error) {
	return m.getDecrypt(ctx, s, ref.DEK, ref.CID, buf)
}

func (m *Machine) checkCache(ref Ref) []byte {
	data, exists := m.cache.Get(ref)
	if !exists {
		return nil
	}
	return data.([]byte)
}

func (m *Machine) loadCache(ref Ref, data []byte) {
	m.cache.Add(ref, append([]byte{}, data...))
}

func (m *Machine) onCacheEvict(key, value any) {
	buf := value.([]byte)
	m.release(buf)
}

func (m *Machine) acquire(n int) []byte {
	buf := m.pool.Get().([]byte)
	if len(buf) < n {
		buf = make([]byte, n)
	}
	return buf
}

func (m *Machine) release(x []byte) {
	x = append(x, make([]byte, cap(x)-len(x))...)
	m.pool.Put(x)
}
