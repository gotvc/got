package metrics

import (
	"bytes"
	"fmt"
	"time"

	"github.com/gotvc/got/src/internal/units"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

type Value struct {
	X     float64
	Units units.Unit
}

func (v Value) String() string {
	return units.FmtFloat64(v.X, v.Units)
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
