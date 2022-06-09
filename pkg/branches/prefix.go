package branches

import (
	"context"
	"strings"

	"github.com/pkg/errors"
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

func (s PrefixSpace) List(ctx context.Context, span Span, limit int) (ret []string, _ error) {
	span2 := Span{
		Begin: s.downward(span.Begin),
		End:   s.downward(span.End),
	}
	stopIter := errors.New("stop iter")
	err := ForEach(ctx, s.Target, span2, func(x string) error {
		if limit > 0 && len(ret) >= limit {
			return stopIter
		}
		y, ok := s.upward(x)
		if !ok {
			// TODO: this should not happen since it would be outside of span2. maybe log?
			return nil
		}
		ret = append(ret, y)
		return nil
	})
	if err != nil && !errors.Is(err, stopIter) {
		return nil, err
	}
	return ret, nil
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
