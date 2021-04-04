package volumes

import "context"

type prefixed struct {
	inner  Realm
	prefix string
}

func NewPrefixed(inner Realm, prefix string) Realm {
	return &prefixed{inner: inner, prefix: prefix}
}

func (r *prefixed) Create(ctx context.Context, name string) error {
	return r.inner.Create(ctx, r.prefix+name)
}

func (r *prefixed) Delete(ctx context.Context, name string) error {
	return r.inner.Delete(ctx, r.prefix+name)
}

func (r *prefixed) Get(ctx context.Context, name string) (*Volume, error) {
	return r.inner.Get(ctx, r.prefix+name)
}

func (r *prefixed) List(ctx context.Context, prefix string) ([]string, error) {
	return r.inner.List(ctx, r.prefix+prefix)
}
