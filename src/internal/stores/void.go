package stores

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
)

var _ Writing = Void{}

type Void struct {
	hf      blobcache.HashFunc
	maxSize int
}

func NewVoid() Void {
	return Void{
		hf:      blobcache.HashAlgo_BLAKE2b_256.HashFunc(),
		maxSize: 1 << 22,
	}
}

func (v Void) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	for i := range dst {
		dst[i] = false
	}
	return nil
}

func (v Void) Post(ctx context.Context, data []byte) (blobcache.CID, error) {
	return v.Hash(data), nil
}

func (v Void) Hash(data []byte) blobcache.CID {
	return v.hf(nil, data)
}

func (v Void) MaxSize() int {
	return v.maxSize
}
