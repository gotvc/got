package ptree

import (
	"fmt"

	"github.com/brendoncarroll/go-state"
)

type Index[T, Ref any] struct {
	Ref Ref
	// IsNatural is true if this index is a natural boundary
	// A natural boundary.
	IsNatural bool

	Span  state.Span[T]
	Count uint
}

func (idx Index[T, Ref]) String() string {
	return fmt.Sprintf("Index{Ref=%v, IsNatural=%v, Span=%v}", idx.Ref, idx.IsNatural, idx.Span)
}

func (idx Index[T, Ref]) Clone(cp func(dst *T, src T)) Index[T, Ref] {
	return Index[T, Ref]{
		Ref:       idx.Ref,
		IsNatural: idx.IsNatural,
		Span:      cloneSpan(idx.Span, cp),
		Count:     idx.Count,
	}
}

func metaIndex[T, Ref any](idx Index[T, Ref]) Index[Index[T, Ref], Ref] {
	span := state.TotalSpan[Index[T, Ref]]()
	span = span.WithLowerIncl(Index[T, Ref]{Span: idx.Span})
	span = span.WithUpperIncl(Index[T, Ref]{Span: idx.Span})
	return Index[Index[T, Ref], Ref]{
		Ref:       idx.Ref,
		IsNatural: idx.IsNatural,
		Span:      span,
		Count:     idx.Count,
	}
}

// flattenIndex turns an index of an index into an index.
func flattenIndex[T, Ref any](x Index[Index[T, Ref], Ref]) Index[T, Ref] {
	return Index[T, Ref]{
		Ref:       x.Ref,
		IsNatural: x.IsNatural,
		Span:      flattenIndexSpan(x.Span),
		Count:     x.Count,
	}
}

func flattenIndexSpan[T, Ref any](x state.Span[Index[T, Ref]]) state.Span[T] {
	lb, lbok, lbi := lowerBound(x)
	if !lbok || !lbi {
		panic("flattenSpan")
	}
	ub, ubok, ubi := upperBound(x)
	if !ubok || !ubi {
		panic("flattenSpan")
	}
	span := state.TotalSpan[T]()
	span = copyLowerBound(span, lb.Span)
	span = copyUpperBound(span, ub.Span)
	return span
}

func compareIndexes[T, Ref any](a, b Index[T, Ref], cmp func(a, b T) int) (ret int) {
	return compareSpan(a.Span, b.Span, cmp)
}

func compareSpan[T any](a, b state.Span[T], cmp func(a, b T) int) int {
	aub, aubok, aubi := upperBound(a)
	blb, blbok, blbi := lowerBound(b)
	if aubok && blbok && !boundsCross(aub, aubi, blb, blbi, cmp) {
		return -1
	}
	alb, albok, albi := lowerBound(a)
	bub, bubok, bubi := upperBound(b)
	if albok && bubok && !boundsCross(bub, bubi, alb, albi, cmp) {
		return 1
	}
	return 0
}

// if lb > rb
// lb is the left bound rb is the right bound
func boundsCross[T any](lb T, lbi bool, rb T, rbi bool, cmp func(a, b T) int) bool {
	if c := cmp(lb, rb); c == 0 && lbi && rbi {
		return true
	} else if c > 0 {
		return true
	}
	return false
}

func lowerBound[T any](x state.Span[T]) (T, bool, bool) {
	lb, ok := x.LowerBound()
	return lb, ok, x.IncludesLower()
}

func upperBound[T any](x state.Span[T]) (T, bool, bool) {
	ub, ok := x.UpperBound()
	return ub, ok, x.IncludesUpper()
}

// copyLowerBound returns x, but with the lower bound from src
func copyLowerBound[T any](x state.Span[T], src state.Span[T]) state.Span[T] {
	lb, lbok, lbi := lowerBound(src)
	if lbok {
		if lbi {
			x = x.WithLowerIncl(lb)
		} else {
			x = x.WithLowerExcl(lb)
		}
	}
	return x
}

// copyUpperBound returns x, but with the upperBound from source
func copyUpperBound[T any](x state.Span[T], src state.Span[T]) state.Span[T] {
	ub, ubok, ubi := upperBound(src)
	if ubok {
		if ubi {
			x = x.WithUpperIncl(ub)
		} else {
			x = x.WithUpperExcl(ub)
		}
	}
	return x
}

func cloneSpan[T any](src state.Span[T], cp func(dst *T, src T)) state.Span[T] {
	span := state.TotalSpan[T]()
	if lb, ok := src.LowerBound(); ok {
		var lb2 T
		cp(&lb2, lb)
		if src.IncludesLower() {
			span = span.WithLowerIncl(lb2)
		} else {
			span = span.WithLowerExcl(lb2)
		}
	}
	if ub, ok := src.UpperBound(); ok {
		var ub2 T
		cp(&ub2, ub)
		if src.IncludesUpper() {
			span = span.WithUpperIncl(ub2)
		} else {
			span = span.WithUpperExcl(ub2)
		}
	}
	return span
}
