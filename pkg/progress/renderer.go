package progress

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"time"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
)

type Renderer struct {
	out   io.Writer
	s     *Reporter
	width int

	eg       errgroup.Group
	stop     chan struct{}
	stopOnce sync.Once
	frame    int
	newLines int
}

func NewRenderer(s *Reporter, out io.Writer) *Renderer {
	r := &Renderer{
		out:  out,
		s:    s,
		stop: make(chan struct{}),
	}
	r.eg.Go(func() error {
		tick := time.NewTicker(500 * time.Millisecond)
		defer tick.Stop()
		last := time.Time{}
		for {
			last = r.maybePrint(last)
			select {
			case <-r.stop:
				return nil
			case <-tick.C:
			}
		}
	})
	return r
}

func (r *Renderer) maybePrint(lastPrint time.Time) time.Time {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if !lastPrint.IsZero() && r.s.last == lastPrint {
		return lastPrint
	}
	r.print(r.s, "")
	return r.s.last
}

const (
	saveCursor    = "\u001B7"
	restoreCursor = "\u001B8"
)

func (r *Renderer) print(x *Reporter, indent string) error {
	buf := &bytes.Buffer{}
	for i := 0; i < r.newLines; i++ {
		clearLine(buf)
		cursorUp(buf, 1)
	}
	fmtReporter(buf, x, indent)
	_, err := r.out.Write(buf.Bytes())
	r.newLines = bytes.Count(buf.Bytes(), []byte("\n"))
	r.frame++
	return err
}

func fmtReporter(buf *bytes.Buffer, x *Reporter, indent string) error {
	buf.WriteString(indent)
	fmt.Fprintf(buf, "%s: ", x.current)
	keys := maps.Keys(x.counters)
	slices.Sort(keys)
	for _, k := range keys {
		c := x.counters[k]
		fmtCounter(buf, &c, k)
		buf.WriteString(", ")
	}
	if len(keys) > 0 {
		buf.WriteString("\n")
	}
	childIDs := maps.Keys(x.children)
	slices.Sort(childIDs)
	for _, cid := range childIDs {
		child := x.children[cid]
		fmtReporter(buf, child, indent+"  ")
	}
	return nil
}

func fmtCounter(buf *bytes.Buffer, c *counter, unit string) error {
	buf.WriteString("(")
	fmtInt64(buf, c.num, unit)
	if c.denom != 0 {
		buf.WriteString("/")
		fmtInt64(buf, c.denom, unit)
	}
	buf.WriteString(")")
	return nil
}

func (r *Renderer) Close() error {
	r.stopOnce.Do(func() {
		close(r.stop)
	})
	return nil
}

func fmtInt64(buf *bytes.Buffer, v int64, unit string) error {
	var prefix string
	var f float64
	switch {
	case v/1e9 > 0:
		prefix = "G"
		f = float64(v) / 1e9
	case v/1e6 > 0:
		prefix = "M"
		f = float64(v) / 1e6
	case v/1e3 > 0:
		prefix = "K"
		f = float64(v) / 1e3
	default:
		_, err := fmt.Fprintf(buf, "%d%s%s", v, prefix, unit)
		return err
	}
	_, err := fmt.Fprintf(buf, "%.2f%s%s", f, prefix, unit)
	return err
}

func cursorUp(b *bytes.Buffer, n int) error {
	fmt.Fprintf(b, "\x1b[%dA", n)
	return nil
}

func clearLine(b *bytes.Buffer) error {
	fmt.Fprintf(b, "\x1b[2K")
	return nil
}
