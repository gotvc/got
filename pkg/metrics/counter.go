package metrics

import (
	"strings"
	"time"

	"github.com/gotvc/got/pkg/units"
)

type Counter struct {
	unit        units.Unit
	first, last time.Time
	num, denom  int64

	delta float64
}

func (c *Counter) Set(now time.Time, x int64, u units.Unit) {
	if c.unit != "" && c.unit != u {
		panic("mismatch units: " + c.unit + " and " + u)
	}
	if c.first.IsZero() {
		c.first = now
	}
	c.unit = u

	c.num = x
	c.delta = float64(x-c.num) / now.Sub(c.last).Seconds()
	c.last = now
}

func (c *Counter) Add(now time.Time, d int64, u units.Unit) int64 {
	if c.unit != "" && c.unit != u {
		panic("mismatch units: " + c.unit + " and " + u)
	}
	if c.first.IsZero() {
		c.first = now
	}
	c.unit = u

	c.num += d
	c.delta = float64(d) / now.Sub(c.last).Seconds()
	c.last = now
	return c.num
}

func (c *Counter) SetDenom(x int64, u units.Unit) {
	if c.unit != "" && c.unit != u {
		panic("mismatch units: " + c.unit + " and " + u)
	}
	c.unit = u

	c.denom = x
}

func (c *Counter) Get() Value {
	return Value{
		X:     float64(c.num),
		Units: c.unit,
	}
}

func (c *Counter) GetDelta() Value {
	last := c.last
	if last.IsZero() {
		last = time.Now()
	}
	return Value{
		X:     float64(c.num) / last.Sub(c.first).Seconds(),
		Units: c.unit + "/s",
	}
}

func (c *Counter) GetDenom() Value {
	return Value{
		X:     float64(c.denom),
		Units: c.unit,
	}
}

func (c *Counter) Absorb(c2 *Counter) {
	if c.unit != "" && c.unit != c2.unit {
		return
	}
	if c.unit == "" {
		c.unit = c2.unit
	}
	if c.first.IsZero() {
		c.first = c2.first
	} else {
		c.first = minTime(c.first, c2.first)
	}
	if c.last.IsZero() {
		c.last = c2.last
	} else {
		c.last = maxTime(c.last, c2.last)
	}
	c.num += c2.num
}

func (c *Counter) String() string {
	b := strings.Builder{}
	b.WriteString("(")
	b.WriteString(c.Get().String())
	if denom := c.GetDenom(); denom.X != 0 {
		b.WriteString("/")
		b.WriteString(denom.String())
	}
	b.WriteString(" Δ=")
	b.WriteString(c.GetDelta().String())
	b.WriteString(")")
	return b.String()
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
