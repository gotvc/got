package gotfsvm

import (
	"context"
	"fmt"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/internal/stores"
)

// Function maps a []gotfs.Root to a gotfs.Root
// Function is a root expression, and the number of inputs it takes.
type Function struct {
	// Arity is the number of inputs the function takes
	Arity uint32
	Expr  Expr
}

// Machine holds configuration for operating on GotFS filesystems.
type Machine struct {
	gotfs *gotfs.Machine
}

func New(fsmach *gotfs.Machine) Machine {
	return Machine{gotfs: fsmach}
}

func (m *Machine) Apply(ctx context.Context, dst [2]stores.RW, fn Function, inputs []Input) (gotfs.Root, error) {
	if len(inputs) != int(fn.Arity) {
		return gotfs.Root{}, fmt.Errorf("function takes %d inputs, have %d", fn.Arity, len(inputs))
	}
	return m.EvalRoot(&EvalCtx{
		Ctx:    ctx,
		Inputs: inputs,
		Dst:    dst,
	}, &fn.Expr)
}

type Input struct {
	Stores [2]stores.Reading
	Root   gotfs.Root
}

// EvalCtx holds the context for evaluating expressions.
type EvalCtx struct {
	Ctx    context.Context
	Dst    [2]stores.RW
	Inputs []Input
}

// Eval evaluates an expression.
func (m *Machine) Eval(ectx *EvalCtx, expr *Expr) (Value, error) {
	ctx := ectx.Ctx
	args := expr.Args

	switch expr.Op {
	case OpCode_Lit:
		return expr.Literal, nil
	case OpCode_Input:
		idx, err := m.evalInt(ectx, args[0])
		if err != nil {
			return nil, err
		}
		if len(ectx.Inputs) <= int(idx) {
			return nil, fmt.Errorf("input index out of bounds %d", idx)
		}
		return &Value_Root{Root: ectx.Inputs[idx].Root}, nil
	case OpCode_SELECT:
		panic("SELECT not yet implemented")
	case OpCode_ShiftOut:
		panic("ShiftOut not yet implemented")
	case OpCode_ShiftIn:
		panic("ShiftIn not yet implemented")
	case OpCode_PICK:
		root, err := m.EvalRoot(ectx, args[0])
		if err != nil {
			return nil, err
		}
		path, err := m.evalPath(ectx, args[1])
		if err != nil {
			return nil, err
		}
		result, err := m.gotfs.Pick(ectx.Ctx, ectx.Dst[1], root, path)
		if err != nil {
			return nil, err
		}
		return &Value_Root{Root: *result}, nil
	case OpCode_PLACE:
		base, err := m.EvalRoot(ectx, args[0])
		if err != nil {
			return nil, err
		}
		path, err := m.evalPath(ectx, args[1])
		if err != nil {
			return nil, err
		}
		mount, err := m.EvalRoot(ectx, args[2])
		if err != nil {
			return nil, err
		}
		result, err := m.gotfs.Graft(ectx.Ctx, ectx.Dst, base, path, mount)
		if err != nil {
			return nil, err
		}
		return &Value_Root{Root: *result}, nil
	case OpCode_CONCAT:

		panic("CONCAT not yet implemented")
	case OpCode_PROMOTE:
		seg, err := m.evalSegment(ectx, args[0])
		if err != nil {
			return nil, err
		}
		root := gotfs.Root{Ref: seg.Root.Ref, Depth: seg.Root.Depth}
		if err := m.gotfs.Check(ctx, ectx.Dst[0], root, func(ref gdat.Ref) error { return nil }); err != nil {
			return nil, err
		}
		return &Value_Root{Root: root}, nil

	case OpCode_EditInfo:
		return nil, fmt.Errorf("EditInfo not yet implemented")
	default:
		return nil, fmt.Errorf("unrecognized op %v", expr.Op)
	}
}

// EvalRoot calls Eval but errors if the result is not a root.
func (m *Machine) EvalRoot(ectx *EvalCtx, expr *Expr) (gotfs.Root, error) {
	val, err := m.Eval(ectx, expr)
	if err != nil {
		return gotfs.Root{}, err
	}
	valroot, ok := val.(*Value_Root)
	if !ok {
		return gotfs.Root{}, fmt.Errorf("expression did not evaluate to a root, got %T", val)
	}
	return valroot.Root, nil
}

func (m *Machine) evalInt(ectx *EvalCtx, x *Expr) (int32, error) {
	val, err := m.Eval(ectx, x)
	if err != nil {
		return 0, err
	}
	idx, ok := val.(*Value_Int)
	if !ok {
		return 0, fmt.Errorf("expected int, got %T", val)
	}
	return int32(*idx), nil
}

func (m *Machine) evalSegment(ectx *EvalCtx, expr *Expr) (*Value_Segment, error) {
	val, err := m.Eval(ectx, expr)
	if err != nil {
		return nil, err
	}
	v, ok := val.(*Value_Segment)
	if !ok {
		return nil, fmt.Errorf("expected segment, got %T", val)
	}
	return v, nil
}

func (m *Machine) evalPath(ectx *EvalCtx, expr *Expr) (string, error) {
	val, err := m.Eval(ectx, expr)
	if err != nil {
		return "", err
	}
	v, ok := val.(*Value_Path)
	if !ok {
		return "", fmt.Errorf("expected path, got %T", val)
	}
	return string(*v), nil
}
