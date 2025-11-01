package branches

import (
	"context"
	"strings"
)

// PrefixSpace is a Space mapped into a prefix under the Target
type PrefixSpace struct {
	Prefix string
	Target Space
}

func NewPrefixSpace(inner Space, prefix string) PrefixSpace {
	return PrefixSpace{
		Target: inner,
		Prefix: prefix,
	}
}

func (s PrefixSpace) Create(ctx context.Context, k string, cfg Params) (*Info, error) {
	return s.Target.Create(ctx, s.downward(k), cfg)
}

func (s PrefixSpace) Inspect(ctx context.Context, k string) (*Info, error) {
	return s.Target.Inspect(ctx, s.downward(k))
}

func (s PrefixSpace) Set(ctx context.Context, k string, cfg Params) error {
	return s.Target.Set(ctx, s.downward(k), cfg)
}

func (s PrefixSpace) Delete(ctx context.Context, k string) error {
	return s.Target.Delete(ctx, s.downward(k))
}

func (s PrefixSpace) List(ctx context.Context, span Span, limit int) ([]string, error) {
	span2 := Span{
		Begin: s.downward(span.Begin),
		End:   s.downward(span.End),
	}
	names, err := s.Target.List(ctx, span2, limit)
	if err != nil {
		return nil, err
	}
	if limit > 0 && len(names) > limit {
		names = names[:limit]
	}
	for i := range names {
		y, ok := s.upward(names[i])
		if !ok {
			// TODO: this should not happen since it would be outside of span2. maybe log?
			continue
		}
		names[i] = y
	}
	return names, nil
}

func (s PrefixSpace) Open(ctx context.Context, name string) (*Branch, error) {
	return s.Target.Open(ctx, s.downward(name))
}

func (s PrefixSpace) downward(x string) string {
	return s.Prefix + x
}

func (s PrefixSpace) upward(x string) (string, bool) {
	y := strings.TrimPrefix(x, s.Prefix)
	if y == x {
		return "", false
	}
	return y, true
}
