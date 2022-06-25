package metrics

import (
	"context"
	"sync"
	"time"

	"github.com/gotvc/got/pkg/units"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

type ctxKey uint8

func WithCollector(ctx context.Context, r *Collector) context.Context {
	if r == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKey(0), r)
}

func FromContext(ctx context.Context) *Collector {
	v := ctx.Value(ctxKey(0))
	if v == nil {
		return nil
	}
	return v.(*Collector)
}

func AddInt(ctx context.Context, m string, n int, u units.Unit) int {
	r := FromContext(ctx)
	return r.AddInt(m, n, u)
}

func SetDenom(ctx context.Context, m string, n int, u units.Unit) {
	r := FromContext(ctx)
	r.SetDenom(m, int64(n), u)
}

func Child(ctx context.Context, name string) (context.Context, func()) {
	c := FromContext(ctx)
	child := c.Child(name)
	return WithCollector(ctx, child), child.Close
}

func Close(ctx context.Context) {
	r := FromContext(ctx)
	r.Close()
}

type Collector struct {
	id      uint
	parent  *Collector
	name    string
	startAt time.Time

	mu       sync.RWMutex
	n        uint
	children map[uint]*Collector
	counters map[string]*Counter
	endAt    time.Time
}

// NewCollector creates a new root collector
func NewCollector() *Collector {
	return &Collector{
		startAt:  time.Now(),
		counters: make(map[string]*Counter),
	}
}

func (r *Collector) AddInt(m string, delta int, u units.Unit) int {
	return int(r.AddInt64(m, int64(delta), u))
}

// AddInt adds delta to the numerator of metric
func (r *Collector) AddInt64(m string, delta int64, u units.Unit) (ret int64) {
	r.updateCounter(m, func(c *Counter) {
		ret = c.Add(time.Now(), delta, u)
	})
	return ret
}

func (r *Collector) SetInt64(m string, x int64, u units.Unit) {
	r.updateCounter(m, func(c *Counter) {
		c.Set(time.Now(), x, u)
	})
}

func (r *Collector) SetDenom(m string, x int64, u units.Unit) {
	r.updateCounter(m, func(c *Counter) {
		c.SetDenom(x, u)
	})
}

func (r *Collector) IsClosed() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return !r.endAt.IsZero()
}

func (r *Collector) GetCounter(m string) *Counter {
	r.mu.RLock()
	defer r.mu.RUnlock()
	agg := &Counter{}
	if c := r.counters[m]; c != nil {
		agg.Absorb(c)
	}
	for _, child := range r.children {
		child.mu.RLock()
		if c := child.counters[m]; c != nil {
			agg.Absorb(c)
		}
		child.mu.RUnlock()
	}
	return agg
}

func (r *Collector) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ks := maps.Keys(r.counters)
	slices.Sort(ks)
	return ks
}

func (r *Collector) Duration() time.Duration {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.endAt.IsZero() {
		return time.Since(r.startAt)
	}
	return r.endAt.Sub(r.startAt)
}

func (r *Collector) ListChildren() []uint {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ks := maps.Keys(r.children)
	slices.Sort(ks)
	return ks
}

func (r *Collector) GetChild(id uint) *Collector {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.children[id]
}

func (r *Collector) Name() string {
	return r.name
}

func (r *Collector) updateCounter(m string, fn func(c *Counter)) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.counters[m]; !exists {
		r.counters[m] = &Counter{}
	}
	fn(r.counters[m])
}

func (r *Collector) Child(name string) *Collector {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.children == nil {
		r.children = make(map[uint]*Collector)
	}
	r.n++
	id := r.n
	r2 := NewCollector()
	r2.parent = r
	r2.id = id
	r2.name = name
	r.children[id] = r2
	return r2
}

func (r *Collector) Close() {
	if r == nil {
		return
	}
	if r.parent == nil {
		return
	}
	r.parent.mu.Lock()
	defer r.parent.mu.Unlock()
	delete(r.parent.children, r.id)
	for k, c1 := range r.counters {
		c2 := r.parent.counters[k]
		if c2 == nil {
			c2 = &Counter{unit: c1.unit}
			r.parent.counters[k] = c2
		} else if c1.unit != c2.unit {
			continue
		}
		c2.Absorb(c1)
	}
}
