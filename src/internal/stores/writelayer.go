package stores

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/state/cadata"
)

type writeLayer struct {
	base    Reading
	writeTo RW
}

func AddWriteLayer(base Reading, writeTo RW) RW {
	return writeLayer{base: base, writeTo: writeTo}
}

func (wl writeLayer) Post(ctx context.Context, data []byte) (cadata.ID, error) {
	return wl.writeTo.Post(ctx, data)
}

func (wl writeLayer) Get(ctx context.Context, id cadata.ID, buf []byte) (int, error) {
	n, err := wl.writeTo.Get(ctx, id, buf)
	if err == nil {
		return n, err
	}
	if !cadata.IsNotFound(err) {
		return 0, err
	}
	return wl.base.Get(ctx, id, buf)
}

func (wl writeLayer) Exists(ctx context.Context, ids []blobcache.CID, dst []bool) error {
	dst2 := make([]bool, len(ids))
	if err := wl.writeTo.Exists(ctx, ids, dst2); err != nil {
		return err
	}
	for i := range dst {
		dst[i] = dst2[i]
	}
	if err := wl.base.Exists(ctx, ids, dst2); err != nil {
		return err
	}
	for i := range dst {
		dst[i] = dst2[i]
	}
	return nil
}

func (wl writeLayer) MaxSize() int {
	size := wl.base.MaxSize()
	if size2 := wl.writeTo.MaxSize(); size2 < size {
		size = size2
	}
	return size
}
