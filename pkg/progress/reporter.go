package progress

import (
	"context"
	"sync"
	"time"
)

type ctxKey uint8

func WithReporter(ctx context.Context, r *Reporter) context.Context {
	if r == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKey(0), r)
}

func FromContext(ctx context.Context) *Reporter {
	v := ctx.Value(ctxKey(0))
	if v == nil {
		return nil
	}
	return v.(*Reporter)
}

func Child(ctx context.Context, r *Reporter) (context.Context, *Reporter) {
	r2 := r.Child()
	return WithReporter(ctx, r2), r2
}

func Close(ctx context.Context) {
	r := FromContext(ctx)
	r.Close()
}

func AddNum(ctx context.Context, m string, n int64) int64 {
	r := FromContext(ctx)
	return r.AddNum(m, n)
}

type counter struct {
	num, denom int64
	last       time.Time
}

type Reporter struct {
	id     uint
	parent *Reporter

	mu       sync.Mutex
	n        uint
	children map[uint]*Reporter

	current  string
	start    time.Time
	counters map[string]counter
	last     time.Time
}

// NewReporter creates a new root reporter
func NewReporter() *Reporter {
	return &Reporter{}
}

// Begin sets the name of the active work item
func (r *Reporter) Begin(x string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.start = time.Now()
	r.current = x
	r.counters = make(map[string]counter)
}

// AddNum adds delta to the numerator of metric
func (r *Reporter) AddNum(m string, delta int64) (ret int64) {
	r.updateCounter(m, func(c *counter) {
		c.num += delta
		ret = c.num
	})
	return ret
}

func (r *Reporter) SetNum(m string, x int64) {
	r.updateCounter(m, func(c *counter) {
		c.num = x
	})
}

func (r *Reporter) SetDenom(m string, x int64) {
	r.updateCounter(m, func(c *counter) {
		c.denom = x
	})
}

func (r *Reporter) updateCounter(m string, fn func(c *counter)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.counters[m]
	fn(&c)
	c.last = time.Now()
	r.counters[m] = c
}

func (r *Reporter) Child() *Reporter {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.children == nil {
		r.children = make(map[uint]*Reporter)
	}
	r.n++
	n := r.n
	r2 := NewReporter()
	r.children[n] = r2
	return r2
}

func (r *Reporter) Close() {
	if r == nil {
		return
	}
	if r.parent != nil {
		r.parent.mu.Lock()
		delete(r.parent.children, r.id)
		r.parent.mu.Unlock()
	}
}
