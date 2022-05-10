package gotobj

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
)

type Builder struct {
	ctx    context.Context
	ms, ds cadata.Store
}

func (o *Operator) NewBuilder(ctx context.Context, ms, ds cadata.Store) *Builder {
	return &Builder{
		ms: ms,
		ds: ds,
	}
}

func (b *Builder) Begin(key []byte, inline bool) error {
	return nil
}

func (b *Builder) Write(p []byte) (int, error) {
	return 0, nil
}

func (b *Builder) CurrentKey(out []byte) []byte {
	return append(out, nil...)
}

func (b *Builder) Finish(context.Context) (*Root, error) {

}
