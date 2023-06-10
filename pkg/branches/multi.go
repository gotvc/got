package branches

import (
	"context"
	"fmt"
	"strings"

	"golang.org/x/exp/slices"
)

type layered []Layer

type Layer struct {
	Prefix string
	Target Space
}

func NewMultiSpace(layers []Layer) (Space, error) {
	for i := 0; i < len(layers); i++ {
		for j := i + 1; j < len(layers); j++ {
			if strings.HasPrefix(layers[j].Prefix, layers[i].Prefix) {
				return nil, fmt.Errorf("layer %d prefix=%s has %d prefix=%s", j, layers[j].Prefix, i, layers[i].Prefix)
			}
		}
	}
	return layered(layers), nil
}

func (r layered) Create(ctx context.Context, k string, md Metadata) (*Branch, error) {
	layer, err := r.find(k)
	if err != nil {
		return nil, err
	}
	l := len(layer.Prefix)
	return layer.Target.Create(ctx, k[l:], md)
}

func (r layered) Delete(ctx context.Context, k string) error {
	layer, err := r.find(k)
	if err != nil {
		return err
	}
	l := len(layer.Prefix)
	return layer.Target.Delete(ctx, k[l:])
}

func (r layered) Set(ctx context.Context, k string, md Metadata) error {
	layer, err := r.find(k)
	if err != nil {
		return err
	}
	l := len(layer.Prefix)
	return layer.Target.Set(ctx, k[l:], md)
}

func (r layered) Get(ctx context.Context, k string) (*Branch, error) {
	layer, err := r.find(k)
	if err != nil {
		return nil, ErrNotExist
	}
	l := len(layer.Prefix)
	return layer.Target.Get(ctx, k[l:])
}

func (r layered) List(ctx context.Context, span Span, limit int) (ret []string, _ error) {
	errs := make([]error, len(r))
	for i, layer := range r {
		if err := ForEach(ctx, layer.Target, span, func(x string) error {
			y := layer.Prefix + x
			if span.Contains(y) {
				ret = append(ret, y)
			}
			slices.Sort(ret)
			if limit > 0 && len(ret) > limit {
				ret = ret[:limit]
			}
			return nil
		}); err != nil {
			errs[i] = err
		}
	}
	var err error
	for i := range errs {
		if errs[i] != nil {
			err = fmt.Errorf("from layer %d: %w", i, errs[i])
			break
		}
	}
	return ret, err
}

func (r layered) find(k string) (Layer, error) {
	for _, layer := range r {
		if strings.HasPrefix(k, layer.Prefix) {
			return layer, nil
		}
	}
	return Layer{}, fmt.Errorf("key not contained in MultiSpace %q", k)
}
