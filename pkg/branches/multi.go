package branches

import (
	"context"
	"strings"

	"github.com/pkg/errors"
)

type layered []Layer

type Layer struct {
	Prefix string
	Target Realm
}

func NewMultiRealm(layers []Layer) (Realm, error) {
	for i := 0; i < len(layers); i++ {
		for j := 0; j < len(layers); j++ {
			if i == j {
				continue
			}
			if strings.HasPrefix(layers[i].Prefix, layers[j].Prefix) {
				return nil, errors.Errorf("MultiRealm: prefix conflict: %q and %q", layers[i], layers[j])
			}
		}
	}
	return layered(layers), nil
}

func (r layered) Create(ctx context.Context, k string) error {
	for _, layer := range r {
		if strings.HasPrefix(layer.Prefix, k) {
			l := len(layer.Prefix)
			return layer.Target.Create(ctx, k[l:])
		}
	}
	return errors.Errorf("key not contained in MultiRealm %q", k)
}

func (r layered) Delete(ctx context.Context, k string) error {
	for _, layer := range r {
		if strings.HasPrefix(layer.Prefix, k) {
			l := len(layer.Prefix)
			return layer.Target.Delete(ctx, k[l:])
		}
	}
	return errors.Errorf("key not contained in MultiRealm %q", k)
}

func (r layered) Get(ctx context.Context, k string) (*Branch, error) {
	for _, layer := range r {
		if strings.HasPrefix(k, layer.Prefix) {
			l := len(layer.Prefix)
			return layer.Target.Get(ctx, k[l:])
		}
	}
	return nil, ErrNotExist
}

func (r layered) ForEach(ctx context.Context, fn func(string) error) error {
	for _, layer := range r {
		if err := layer.Target.ForEach(ctx, func(x string) error {
			return fn(layer.Prefix + x)
		}); err != nil {
			return err
		}
	}
	return nil
}
