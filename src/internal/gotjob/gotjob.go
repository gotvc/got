package gotjob

import (
	"context"

	"github.com/gotvc/got/src/internal/metrics"
	"golang.org/x/sync/errgroup"
)

// Ctx is available to all jobs
type Ctx struct {
	context.Context
	cf context.CancelFunc
	eg errgroup.Group
}

func New(ctx context.Context) Ctx {
	ctx, cf := context.WithCancel(ctx)
	return Ctx{
		Context: ctx,
		cf:      cf,
	}
}

func (jc *Ctx) Child(name string) Ctx {
	ctx, cf := metrics.Child(jc, name)
	return Ctx{
		Context: ctx,
		cf:      cf,
		eg:      errgroup.Group{},
	}
}

func (ctx *Ctx) Go(f func() error) {
	ctx.eg.Go(f)
}

func (ctx *Ctx) Wait() error {
	return ctx.eg.Wait()
}
