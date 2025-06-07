package stores

import (
	"context"

	"go.brendoncarroll.net/state/cadata"
)

type writeLayer struct {
	base, writeTo cadata.Store
}

func AddWriteLayer(base cadata.Store, writeTo cadata.Store) cadata.Store {
	if base.Hash(nil) != writeTo.Hash(nil) {
		panic("write layer has different hash function than base")
	}
	return writeLayer{base: AssertReadOnly(base), writeTo: writeTo}
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
	return wl.base.Exists(ctx, id)
}

func (wl writeLayer) Delete(ctx context.Context, id cadata.ID) error {
	return wl.writeTo.Delete(ctx, id)
}

func (wl writeLayer) List(ctx context.Context, span cadata.Span, ids []cadata.ID) (int, error) {
	// TODO: maybe list both
	return wl.base.List(ctx, span, ids)
}

func (wl writeLayer) MaxSize() int {
	size := wl.base.MaxSize()
	if size2 := wl.writeTo.MaxSize(); size2 < size {
		size = size2
	}
	return size
}

func (wl writeLayer) Hash(x []byte) cadata.ID {
	return wl.base.Hash(x)
}
