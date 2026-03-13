package stores

import (
	"context"
	"errors"

	"blobcache.io/blobcache/src/blobcache"
)

type Overlay struct {
	base    Reading
	writeTo RW
}

func NewOverlay(base Reading, writeTo RW) RW {
	return Overlay{base: base, writeTo: writeTo}
}

func (wl Overlay) Post(ctx context.Context, data []byte) (blobcache.CID, error) {
	return wl.writeTo.Post(ctx, data)
}

func (wl Overlay) Get(ctx context.Context, id blobcache.CID, buf []byte) (int, error) {
	n, err := wl.writeTo.Get(ctx, id, buf)
	if err == nil {
		return n, err
	}
	if !isNotFound(err) {
		return 0, err
	}
	return wl.base.Get(ctx, id, buf)
}

func (wl Overlay) Exists(ctx context.Context, ids []blobcache.CID, dst []bool) error {
	dst2 := make([]bool, len(ids))
	if err := wl.writeTo.Exists(ctx, ids, dst2); err != nil {
		return err
	}
	for i := range dst {
		dst[i] = dst[i] || dst2[i]
	}
	for i := range dst2 {
		dst2[i] = false
	}
	if err := wl.base.Exists(ctx, ids, dst2); err != nil {
		return err
	}
	for i := range dst {
		dst[i] = dst[i] || dst2[i]
	}
	return nil
}

func (wl Overlay) MaxSize() int {
	size := wl.base.MaxSize()
	if size2 := wl.writeTo.MaxSize(); size2 < size {
		size = size2
	}
	return size
}

func isNotFound(err error) bool {
	e2 := &blobcache.ErrNotFound{}
	return errors.As(err, &blobcache.ErrNotFound{}) || errors.As(err, &e2)
}
