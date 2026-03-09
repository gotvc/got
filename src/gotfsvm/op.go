package gotfsvm

import (
	"fmt"
)

// OpCode is just the arity and opcode bits.
// OpCode can be OR'd directly into an I to set the op and arity.
type OpCode uint32

// 0-Arity
const (
	OpCode_UNKNOWN = 0

	// OpCode_Nat produces a 32 bit Nat from 24 bits of data stored in the instruction.
	OpCode_Nat = 32 << 24
)

// 1-Arity
const (
	op1ArityOffset = (1 << 30) | iota<<24
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
