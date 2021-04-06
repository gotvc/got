package gdat

import (
	"context"
	"hash/crc64"
	"io"

	"github.com/blobcache/blobcache/pkg/bccrypto"
	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/brendoncarroll/got/pkg/cadata"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"
)

const maxNodeSize = blobs.MaxSize

type Store = cadata.Store

type Option = func(*Operator)

func WithEncryptionKeyFunc(kf bccrypto.KeyFunc) Option {
	return func(o *Operator) {
		o.kf = kf
	}
}

func WithCacheSize(n int) Option {
	return func(o *Operator) {
		o.cacheSize = n
	}
}

type Operator struct {
	kf bccrypto.KeyFunc

	cacheSize int
	cache     *lru.ARCCache
}

func NewOperator(opts ...Option) *Operator {
	o := &Operator{
		kf:        bccrypto.SaltedConvergent(nil),
		cacheSize: 16,
	}
	for _, opt := range opts {
		opt(o)
	}
	var err error
	if o.cache, err = lru.NewARC(o.cacheSize); err != nil {
		panic(err)
	}
	return o
}

func (o *Operator) Post(ctx context.Context, s Store, data []byte) (*Ref, error) {
	if len(data) > maxNodeSize {
		return nil, errors.Errorf("data len=%d exceeds max size", len(data))
	}
	kf := bccrypto.SaltedConvergent(nil)
	id, dek, err := bccrypto.Post(ctx, s, kf, data)
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
	return bccrypto.GetF(ctx, s, ref.DEK, ref.CID, func(data []byte) error {
		o.loadCache(ref, data)
		return fn(data)
	})
}

func (o *Operator) Read(ctx context.Context, s Store, ref Ref, buf []byte) (int, error) {
	var n int
	err := o.GetF(ctx, s, ref, func(data []byte) error {
		n = copy(buf, data)
		if n < len(buf) {
			return io.ErrShortBuffer
		}
		return nil
	})
	return n, err
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

func assertNotModified(data []byte, fn func(data []byte) error) error {
	before := crc64Sum(data)
	err := fn(data)
	after := crc64Sum(data)
	if before != after {
		panic("buffer modified")
	}
	return err
}

func crc64Sum(data []byte) uint64 {
	return crc64.Checksum(data, crc64.MakeTable(crc64.ISO))
}
