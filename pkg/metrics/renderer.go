package metrics

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
	out io.Writer
	s   *Collector

	eg       errgroup.Group
	stop     chan struct{}
	stopOnce sync.Once
	frame    int
	newLines int
}

func NewTTYRenderer(s *Collector, out io.Writer) *Renderer {
	r := &Renderer{
		out:  out,
		s:    s,
		stop: make(chan struct{}),
	}
	r.eg.Go(func() error {
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		last := time.Time{}
		for {
			last = r.maybePrint(last)
			select {
			case <-r.stop:
				return r.clear()
			case <-tick.C:
			}
		}
	})
	return r
}

func (r *Renderer) maybePrint(lastPrint time.Time) time.Time {
	// if !lastPrint.IsZero() && r.s.last == lastPrint {
	// 	return lastPrint
	// }
	r.print(r.s, "")
	return time.Now()
}

func (r *Renderer) clear() error {
	buf := &bytes.Buffer{}
	clearLinesUp(buf, r.newLines)
	fmt.Fprintln(buf, r.s.GetPrevious())
	_, err := r.out.Write(buf.Bytes())
	return err
}

func (r *Renderer) print(x *Collector, indent string) error {
	buf := &bytes.Buffer{}
	clearLinesUp(buf, r.newLines)
	fmtReporter(buf, x, indent)
	_, err := r.out.Write(buf.Bytes())
	r.newLines = bytes.Count(buf.Bytes(), []byte("\n"))
	r.frame++
	return err
}

func fmtReporter(buf *bytes.Buffer, x *Collector, indent string) error {
	buf.WriteString(indent)
	if current := x.GetCurrent(); current != "" {
		fmt.Fprintf(buf, "%s: ", current)
	} else if sum := x.GetPrevious(); sum != nil {
		fmt.Fprintf(buf, "%v", sum)
		return nil
	} else {
		return nil
	}
	var i int
	x.forEachCounter(func(name string, c *counter) {
		if i > 0 {
			buf.WriteString(", ")
		}
		i++
		fmtCounter(buf, name, c)
	})
	buf.WriteString("\n")

	childIDs := maps.Keys(x.children)
	slices.Sort(childIDs)
	for _, cid := range childIDs {
		child := x.children[cid]
		fmtReporter(buf, child, indent+"  ")
	}
	return nil
}

func fmtCounter(buf *bytes.Buffer, name string, c *counter) error {
	_, err := fmt.Fprintf(buf, "%s=%v", name, c)
	return err
	return nil
}

func (r *Renderer) Close() error {
	r.stopOnce.Do(func() {
		close(r.stop)
	})
	r.eg.Wait()
	return nil
}

func cursorUp(b *bytes.Buffer, n int) error {
	fmt.Fprintf(b, "\x1b[%dA", n)
	return nil
}

func clearLine(b *bytes.Buffer) error {
	fmt.Fprintf(b, "\x1b[2K")
	return nil
}

func clearLinesUp(b *bytes.Buffer, n int) error {
	for i := 0; i < n; i++ {
		cursorUp(b, 1)
		clearLine(b)
	}
	return nil
}
