package gotfsvm

import (
	"fmt"
	"iter"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv"
)

type OpCode uint32

const (
	OpCode_UNKNOWN = iota

	// Literal is a literally specified Value
	// () -> (Value)
	OpCode_Lit

	// Input refers to a previoius snapshot by index
	// (index) -> (Root)
	// This Op reads from the entire input at the index.
	OpCode_Input

	// Select produces a segment from a Span within a Root
	// (Root, Span) -> (Segment)
	// This Op reads only from the Span within root.
	OpCode_SELECT
	// Promote checks a segment for consistency and returns a root.
	// (Segment) -> Root
	OpCode_PROMOTE

	// ShiftOut shifts a segment out to a path
	// (Segment, Path) -> (Segment)
	OpCode_ShiftOut
	// ShiftIn shifts a segment by removing Path
	// It errors if not every path at that segment has Path as a prefix
	// (Segment, Path) -> (Segment)
	OpCode_ShiftIn
	// Pick produces a Root, taken from a specific path within another Root
	// (Root, Path) -> (Root)
	OpCode_PICK
	// Place takes a root, a path and a second root, and places the second root at that path in the first root.
	// The parent of that path must already exist in root.
	// (Root, Path, Root) -> Root
	OpCode_PLACE
	// Concat takes 2 segments and concatenates them to produce a larger segment
	// (Segment, Segment) -> Segment
	OpCode_CONCAT

	OpCode_EditInfo
)

func (o OpCode) String() string {
	switch o {
	case OpCode_Lit:
		return "lit"
	case OpCode_Input:
		return "input"
	case OpCode_SELECT:
		return "select"
	case OpCode_ShiftOut:
		return "shiftout"
	case OpCode_ShiftIn:
		return "shiftin"
	case OpCode_PICK:
		return "pick"
	case OpCode_PLACE:
		return "place"
	case OpCode_CONCAT:
		return "concat"
	case OpCode_PROMOTE:
		return "promote"
	case OpCode_EditInfo:
		return "editinfo"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", o)
	}
}

type Expr struct {
	Op   OpCode
	Args [3]*Expr
	// If the OpCode is for a literal then it will be provided here, otherwise it will be nil.
	Literal Value
}

func (e *Expr) Pretty(out []byte, indent int) []byte {
	out = append(out, '(')
	out = append(out, e.Op.String()...)
	first := true
	for _, arg := range e.Args {
		if arg == nil {
			continue
		}
		if !first {
			out = append(out, '\n')
		}
		first = false
		for range indent {
			out = append(out, ' ')
		}
		out = arg.Pretty(out, indent+1)
	}
	out = append(out, ')')
	return out
}

func (e *Expr) String() string {
	return string(e.Pretty(nil, 0))
}

func (e *Expr) ReadsFrom() iter.Seq[ReadSpan] {
	switch e.Op {
	case OpCode_CONCAT:
		return iterConcat(e.Args[0].ReadsFrom(), e.Args[1].ReadsFrom())
	case OpCode_Input:
		validx := e.Args[0].Literal.(*Value_Int)
		return iterUnit(ReadSpan{
			Index: int(*validx),
			Span:  gotkv.TotalSpan(),
		})
	case OpCode_SELECT, OpCode_ShiftOut, OpCode_ShiftIn, OpCode_PICK, OpCode_PROMOTE:
		return e.Args[0].ReadsFrom()
	case OpCode_PLACE:
		return iterConcat(e.Args[0].ReadsFrom(), e.Args[2].ReadsFrom())
	default:
		return iterEmpty[ReadSpan]()
	}
}

func (e *Expr) WritesTo() iter.Seq[gotfs.Span] {
	switch e.Op {
	default:
		return iterEmpty[gotfs.Span]()
	}
}

func Pick(root *Expr, path *Expr) *Expr {
	return &Expr{Op: OpCode_PICK, Args: [3]*Expr{root, path, nil}}
}

func Place(base *Expr, path *Expr, mount *Expr) *Expr {
	return &Expr{Op: OpCode_PLACE, Args: [3]*Expr{base, path, mount}}
}

// ReadSpan is a region that the expression reads from in the input.
type ReadSpan struct {
	// Index is the number of the previous root.
	Index int
	// Span is the region of the Input at Index that was read.
	Span gotfs.Span
}

func iterEmpty[T any]() iter.Seq[T] {
	return func(yield func(T) bool) {}
}

func iterConcat[T any](its ...iter.Seq[T]) iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, it := range its {
			for x := range it {
				if !yield(x) {
					return
				}
			}
		}
	}
}

func iterUnit[T any](x T) iter.Seq[T] {
	return func(yield func(T) bool) {
		yield(x)
	}
}
