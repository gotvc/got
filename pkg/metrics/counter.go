package metrics

import (
	"strings"
	"time"

	"github.com/gotvc/got/pkg/units"
)

type Counter struct {
	unit       units.Unit
	num, denom int64

	deltaNum int64
	deltaDur time.Duration
	last     time.Time
}

func (c *Counter) Set(now time.Time, x int64, u units.Unit) {
	if c.unit != "" && c.unit != u {
		panic("mismatch units: " + c.unit + " and " + u)
	}
	c.unit = u

	c.deltaNum = c.num - x
	c.num = x
	c.deltaDur = now.Sub(c.last)
	c.last = now
}

func (c *Counter) Add(now time.Time, d int64, u units.Unit) int64 {
	if c.unit != "" && c.unit != u {
		panic("mismatch units: " + c.unit + " and " + u)
	}
	c.unit = u

	c.deltaNum = d
	c.deltaDur = now.Sub(c.last)
	c.num += d
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
	return Value{
		X:     float64(c.deltaNum) / c.deltaDur.Seconds(),
		Units: c.unit + "/s",
	}
}

func (c *Counter) GetDenom() Value {
	return Value{
		X:     float64(c.num),
		Units: c.unit,
	}
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
