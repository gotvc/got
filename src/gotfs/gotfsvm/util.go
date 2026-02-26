package gotfsvm

import "github.com/gotvc/got/src/gotfs"

func ChangesOnBase(segs []gotfs.Segment) *Expr {
	var exprs []*Expr
	for _, seg := range segs {
		exprs = append(exprs, seg)
	}
	return Concat(exprs)
}
