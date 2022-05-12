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

func (s PrefixSpace) Create(ctx context.Context, k string, p Params) (*Branch, error) {
	return s.Target.Create(ctx, s.downward(k), p)
}

func (s PrefixSpace) Get(ctx context.Context, k string) (*Branch, error) {
	return s.Target.Get(ctx, s.downward(k))
}

func (s PrefixSpace) Delete(ctx context.Context, k string) error {
	return s.Target.Delete(ctx, s.downward(k))
}

func (s PrefixSpace) ForEach(ctx context.Context, span Span, fn func(string) error) error {
	span2 := Span{
		Begin: s.downward(span.Begin),
		End:   s.downward(span.End),
	}
	return s.Target.ForEach(ctx, span2, func(x string) error {
		y, ok := s.upward(x)
		if !ok {
			// TODO: this should not happen since it would be outside of span2. maybe log?
			return nil
		}
		return fn(y)
	})
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
