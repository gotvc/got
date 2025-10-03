package stores

import (
	"context"

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

func (wl writeLayer) Exists(ctx context.Context, id cadata.ID) (bool, error) {
	exists, err := wl.writeTo.Exists(ctx, id)
	if err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}
	if e, ok := wl.base.(cadata.Exister); ok {
		return e.Exists(ctx, id)
	} else {
		panic("base store does not support Exists")
	}
}

func (wl writeLayer) MaxSize() int {
	size := wl.base.MaxSize()
	if size2 := wl.writeTo.MaxSize(); size2 < size {
		size = size2
	}
	return size
}
