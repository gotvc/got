package branches

import (
	"context"

	"github.com/gotvc/got/pkg/cells/pkecell"
)

type PKESpace struct {
	Inner  Space
	Params pkecell.ParamsV1

	mdDown   func(Metadata) Metadata
	nameDown func(string) string
}

func (s *PKESpace) Create(ctx context.Context, name string, md Metadata) (*Branch, error) {
	return s.Inner.Create(ctx, s.nameDown(name), s.mdDown(md))
}

func (s *PKESpace) Set(ctx context.Context, name string, md Metadata) error {
	return s.Inner.Set(ctx, s.nameDown(name), s.mdDown(md))
}

func (s *PKESpace) Get(ctx context.Context, name string) (*Branch, error) {
	b, err := s.Inner.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	panic(b)
}

func (s *PKESpace) Delete(ctx context.Context, name string) error {
	panic("")
}

func (s *PKESpace) List(ctx context.Context, span Span) ([]string, error) {
	panic("")
}
