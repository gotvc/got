package gotrope

import (
	"golang.org/x/exp/slices"
)

type Weight []uint64

// Add sets w to be equal to left + right
// left or right may be w.
// Add is not commutative
func (w *Weight) Add(left, right Weight) {
	if len(right) == 0 {
		panic(right)
	}
	if len(right) > 1 {
		panic(right) // TODO: support len > 1
	}

	switch {
	case len(left) == 0:
		w.Set(right)
	default:
		w.Set(Weight{left[0] + right[0]})
	}
}

func (w *Weight) Set(x Weight) {
	*w = append((*w)[:0], x...)
}

func (w Weight) Clone() Weight {
	return append(Weight{}, w...)
}

type Path []uint64

func (p *Path) Add(left, right Weight) {
	w := (*Weight)(p)
	w.Add(left, right)
}

func PathCompare(a, b Path) int {
	return slices.Compare(a, b)
}
