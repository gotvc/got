package progress

import (
	"context"
	"io"
	"sync"

	"golang.org/x/sync/errgroup"
)

type ctxKey uint8

func ContextWith(ctx context.Context, r *Reporter) context.Context {
	return context.WithValue(ctx, ctxKey(0), r)
}

func FromContext(ctx context.Context) *Reporter {
	v := ctx.Value(ctxKey(0))
	if v == nil {
		return nil
	}
	return v.(*Reporter)
}

type Reporter struct {
	out  io.Writer
	stop chan struct{}
	eg   errgroup.Group

	mu      sync.Mutex
	current string
	num     int64
	denom   int64
}

func NewReporter(out io.Writer) *Reporter {
	r := &Reporter{
		out: out,
	}
	r.eg.Go(func() error {
		select {}
	})
}

func (r *Reporter) SetCurrent(x string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.current = x
}

func (r *Reporter) AddNum(x int64) int64 {
	r.num = x
	return r.num
}

func (r *Reporter) SetNum(x int64) {
	r.num = x
}

func (r *Reporter) SetDenom(x int64) {
	r.denom = x
}
