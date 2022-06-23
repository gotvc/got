package metrics

import (
	"bytes"
	"context"
	"fmt"
	"strings"
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

// Track starts tracking metrics under a work item named x, if the context has a collector.
func Track(ctx context.Context, x string) func() {
	r := FromContext(ctx)
	return r.Begin(x)
}

func AddInt(ctx context.Context, m string, n int, u units.Unit) int {
	r := FromContext(ctx)
	return r.AddInt(m, n, u)
}

func SetDenom(ctx context.Context, m string, n int, u units.Unit) {
	r := FromContext(ctx)
	r.SetDenom(m, int64(n), u)
}

func Child(ctx context.Context) context.Context {
	r := FromContext(ctx)
	return WithCollector(ctx, r.Child())
}

func Close(ctx context.Context) {
	r := FromContext(ctx)
	r.Close()
}

type counter struct {
	units      units.Unit
	num, denom int64
	deltaNum   int64
	deltaDur   time.Duration
	last       time.Time
}

func (c *counter) String() string {
	buf := &bytes.Buffer{}
	buf.WriteString("(")
	buf.WriteString(units.FmtFloat64(float64(c.num), c.units))
	if c.denom != 0 {
		buf.WriteString("/")
		buf.WriteString(units.FmtFloat64(float64(c.denom), c.units))
	}
	dx, dxp := units.SIPrefix(float64(c.deltaNum))
	fmt.Fprintf(buf, " Δ=%.2f%s/s", dx, dxp+string(c.units))
	buf.WriteString(")")
	return buf.String()
}

type Value struct {
	X     float64
	Units units.Unit
}

// Summary is a summary of a completed task
type Summary struct {
	Name    string
	StartAt time.Time
	EndAt   time.Time
	Metrics map[string]Value
}

func (s *Summary) String() string {
	b := &bytes.Buffer{}
	fmt.Fprintf(b, "%s: done in %v ", s.Name, s.Elapsed())
	keys := maps.Keys(s.Metrics)
	slices.Sort(keys)
	for i, k := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		v := s.Metrics[k]
		b.WriteString(units.FmtFloat64(v.X, v.Units))
	}
	return b.String()
}

func (s *Summary) Elapsed() time.Duration {
	return s.EndAt.Sub(s.StartAt)
}

func (s *Summary) GetDelta(k string) float64 {
	return s.Metrics[k].X / s.Elapsed().Seconds()
}

type layer struct {
	name     string
	counters map[string]counter
	startAt  time.Time
}

func newLayer(x string) layer {
	return layer{
		name:     x,
		counters: make(map[string]counter),
		startAt:  time.Now(),
	}
}

type Collector struct {
	id     uint
	parent *Collector

	mu       sync.RWMutex
	n        uint
	children map[uint]*Collector
	stack    []layer
	prev     *Summary
}

// NewCollector creates a new root collector
func NewCollector() *Collector {
	return &Collector{}
}

// Begin sets the name of the active work item
func (r *Collector) Begin(x string) func() {
	if r == nil {
		return func() {}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stack = append(r.stack, newLayer(x))
	return func() {
		r.End(x)
	}
}

func (r *Collector) End(x string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	l := len(r.stack)
	if l == 0 {
		panic(x)
	}
	if r.stack[l-1].name != x {
		panic(fmt.Sprintf("tried to end %q, but %q is still on the stack", x, r.stack[l-1].name))
	}
	startAt := r.stack[l-1].startAt
	counters := r.stack[l-1].counters
	sum := &Summary{
		Name:    x,
		StartAt: startAt,
		EndAt:   now,
		Metrics: map[string]Value{},
	}
	for m, c := range counters {
		sum.Metrics[m] = Value{X: float64(c.num), Units: c.units}
	}
	r.prev = sum
	r.stack = r.stack[:l-1]
}

// AddInt adds delta to the numerator of metric
func (r *Collector) AddInt(m string, delta int, u units.Unit) (ret int) {
	r.updateCounter(m, true, func(c *counter) {
		c.units = u
		c.num += int64(delta)
		ret = int(c.num)
	})
	return ret
}

func (r *Collector) Set(m string, x int64) {
	r.updateCounter(m, false, func(c *counter) {
		c.num = x
	})
}

func (r *Collector) SetDenom(m string, x int64, u units.Unit) {
	r.updateCounter(m, false, func(c *counter) {
		c.denom = x
		c.units = u
	})
}

func (r *Collector) forEachCounter(fn func(name string, c *counter)) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.stack) < 1 {
		return
	}
	counters := r.stack[0].counters
	keys := maps.Keys(counters)
	slices.Sort(keys)
	for _, k := range keys {
		c := counters[k]
		fn(k, &c)
	}
}

func (r *Collector) updateCounter(m string, all bool, fn func(c *counter)) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.stack) < 1 {
		panic("metrics: data before Begin")
	}
	layers := r.stack
	if !all {
		layers = r.stack[len(r.stack)-2:]
	}
	for _, lay := range layers {
		counters := lay.counters
		c := counters[m]
		prevNum := c.num
		prevLast := c.last
		fn(&c)
		c.last = time.Now()
		c.deltaNum = c.num - prevNum
		c.deltaDur = c.last.Sub(prevLast)
		counters[m] = c
	}
}

func (r *Collector) GetCurrent() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	sb := strings.Builder{}
	for i, l := range r.stack {
		if i > 0 {
			sb.WriteString(": ")
		}
		sb.WriteString(l.name)
	}
	return sb.String()
}

func (r *Collector) GetPrevious() *Summary {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.prev
}

func (r *Collector) Child() *Collector {
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
	r.children[id] = r2
	return r2
}

func (r *Collector) Close() {
	if r == nil {
		return
	}
	if r.parent != nil {
		r.parent.mu.Lock()
		delete(r.parent.children, r.id)
		r.parent.mu.Unlock()
	}
}
