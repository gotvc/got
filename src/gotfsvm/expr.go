package gotfsvm

import (
	"fmt"
	"iter"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv"
)

// OpCode is just the arity and opcode bits.
// OpCode can be OR'd directly into an I to set the op and arity.
type OpCode uint32

// 0-Arity
const (
	OpCode_UNKNOWN = 0

	op0ArityOffset = iota<<24 | (0 << 30)

	// OpCode_Nat produces a 32 bit Nat from 24 bits of data stored in the instruction.
	OpCode_Nat = 32 << 24
)

// 1-Arity
const (
	op1ArityOffset = iota<<24 | (1 << 30)
	// Input refers to a previoius snapshot by index
	// (index) -> (Root)
	// This Op reads from the entire input at the index.
	OpCode_Input
	// OpCode_Data loads from an index in the data table.
	// (index) -> (Value)
	OpCode_Data

	// Promote checks a segment for consistency and returns a root.
	// (Segment) -> Root
	OpCode_PROMOTE
)

// 2-Arity
const (
	op2ArityOffset = iota<<24 | (2 << 30)
	// Select produces a segment from a Span within a Root
	// (Root, Span) -> (Segment)
	// This Op reads only from the Span within root.
	OpCode_SELECT

	// Concat takes 2 segments and concatenates them to produce a larger segment
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

	// (Segment, Segment) -> Segment
	OpCode_CONCAT
)

const (
	op3ArityOffset = (iota << 24) | 3<<30

	// Place takes a root, a path and a second root, and places the second root at that path in the first root.
	// The parent of that path must already exist in root.
	// (Root, Path, Root) -> Root
	OpCode_PLACE

	// MkdirAll creates the directory at path and any of its ancestors if necessary.
	// (Root, Path, FileMode) -> Root
	OpCode_MKDIRALL
)

func (o OpCode) Arity() int {
	return int(o >> 30)
}

func (o OpCode) String() string {
	switch o {
	case OpCode_Data:
		return "data"
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
	case OpCode_MKDIRALL:
		return "mkdirall"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", o)
	}
}

type Expr struct {
	Op   OpCode
	Args [3]*Expr
	// If the expression takes a value from the data table then that will be included here.
	Data Value
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

// Passthrough returns the ReadSpans that will not be changed from the input.
func (e *Expr) Passthrough() iter.Seq[ReadSpan] {
	switch e.Op {
	case OpCode_CONCAT:
		return iterConcat(e.Args[0].Passthrough(), e.Args[1].Passthrough())
	case OpCode_Input:
		validx := e.Args[0].Data.(*Value_Nat)
		return iterUnit(ReadSpan{
			Index: int(*validx),
			Span:  gotkv.TotalSpan(),
		})
	case OpCode_SELECT, OpCode_ShiftOut, OpCode_ShiftIn, OpCode_PICK, OpCode_PROMOTE:
		return e.Args[0].Passthrough()
	case OpCode_PLACE:
		return iterConcat(e.Args[0].Passthrough(), e.Args[2].Passthrough())
	case OpCode_MKDIRALL:
		return e.Args[0].Passthrough()
	default:
		return iterEmpty[ReadSpan]()
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

func getExprArity(x *Expr) (ret uint32, _ error) {
	if x == nil {
		return 0, nil
	}
	switch x.Op {
	case OpCode_Input:
		idxExpr := x.Args[0]
		if idxExpr == nil {
			return 0, fmt.Errorf("invalid expr, cannot get arity")
		}
		val := idxExpr.Data
		if val == nil {
			return 0, fmt.Errorf("missin literal, cannot get arity")
		}
		idx, ok := val.(Value_Nat)
		if !ok {
			return 0, fmt.Errorf("expected nat literal, got %T", val)
		}
		return uint32(idx) + 1, nil
	default:
		for _, arg := range x.Args {
			a2, err := getExprArity(arg)
			if err != nil {
				return 0, err
			}
			ret = max(ret, a2)
		}
	}
	return ret, nil
}

func Concat(xs ...*Expr) *Expr {
	switch len(xs) {
	case 0:
		return &Expr{}
	case 1:
		return xs[0]
	case 2:
		return &Expr{Op: OpCode_CONCAT, Args: [3]*Expr{xs[0], xs[1]}}
	default:
		l := Concat(xs[:len(xs)/2]...)
		r := Concat(xs[len(xs)/2:]...)
		return &Expr{Op: OpCode_CONCAT, Args: [3]*Expr{l, r}}
	}
}
