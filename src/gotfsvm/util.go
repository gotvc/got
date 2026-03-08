package gotfsvm

import (
	"os"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv"
)

func RootExpr(root gotfs.Root) *Expr {
	return Literal(&Value_Root{root})
}

func InputExpr(index int) *Expr {
	idx := Value_Nat(index)
	return &Expr{Op: OpCode_Input, Args: [3]*Expr{Literal(idx)}}
}

func PromoteExpr(seg *Expr) *Expr {
	return &Expr{Op: OpCode_PROMOTE, Args: [3]*Expr{seg}}
}

func Literal(val Value) *Expr {
	var op OpCode
	if _, ok := val.(Value_Nat); ok {
		op = OpCode_Nat
	} else {
		op = OpCode_Data
	}
	return &Expr{Op: op, Data: val}
}

func MkdirAllExpr(root *Expr, path string, mode os.FileMode) *Expr {
	p := Value_Path(path)
	m := Value_FileMode(mode)
	return &Expr{Op: OpCode_MKDIRALL, Args: [3]*Expr{
		root,
		Literal(&p),
		Literal(m),
	}}
}

// ChangesOnBase builds a Concat expression that applies changes on top of base.
// It inserts Select expressions from base between each change segment.
func ChangesOnBase(base *Expr, changes []gotfs.Segment) *Expr {
	var exprs []*Expr
	for i := range changes {
		var baseSpan gotkv.Span
		if i > 0 {
			baseSpan.Begin = changes[i-1].Span.End
		}
		baseSpan.End = changes[i].Span.Begin
		exprs = append(exprs, selectExpr(base, baseSpan))
		exprs = append(exprs, litSegment(changes[i]))
	}
	if len(exprs) > 0 {
		exprs = append(exprs, selectExpr(base, gotkv.Span{
			Begin: changes[len(changes)-1].Span.End,
			End:   nil,
		}))
	}
	return Concat(exprs...)
}

func selectExpr(root *Expr, span gotkv.Span) *Expr {
	return &Expr{
		Op: OpCode_SELECT,
		Args: [3]*Expr{
			root,
			{Op: OpCode_Data, Data: &Value_Span{Span: gotfs.Span(span)}},
		},
	}
}

func litSegment(seg gotfs.Segment) *Expr {
	vs := Value_Segment{seg}
	return &Expr{Op: OpCode_Data, Data: &vs}
}
