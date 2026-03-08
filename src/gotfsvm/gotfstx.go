package gotfsvm

import (
	"context"
	"fmt"
	"os"
	"slices"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/internal/gotjob"
	"github.com/gotvc/got/src/internal/stores"
)

// Machine holds configuration for operating on GotFS filesystems.
type Machine struct {
	gotfs *gotfs.Machine
	gdat  *gdat.Machine
}

func New(fsmach *gotfs.Machine) Machine {
	return Machine{gotfs: fsmach, gdat: gdat.NewMachine()}
}

// Function maps a []gotfs.Root to a gotfs.Root
// Function is a root expression, and the number of inputs it takes.
type Function struct {
	// Arity is the number of inputs the function takes
	Arity uint32
	Ref   gdat.Ref
}

// NewFunction creates a new function from an expression.
func (m *Machine) NewFunction(ctx context.Context, s stores.Writing, body Expr) (Function, error) {
	arity, err := getExprArity(&body)
	if err != nil {
		return Function{}, err
	}
	w := NewIWriter(m)
	if _, err := w.WriteExpr(&body); err != nil {
		return Function{}, err
	}
	ref, err := w.Flush(ctx, s)
	if err != nil {
		return Function{}, err
	}
	return Function{
		Arity: arity,
		Ref:   ref,
	}, nil
}

func (m *Machine) getBody(ctx context.Context, s stores.Reading, fn Function) (*Expr, error) {
	var body *Expr
	err := m.gdat.GetF(ctx, s, fn.Ref, func(data []byte) error {
		var err error
		body, err = parseFunctionBody(ctx, m.gdat, s, data)
		return err
	})
	return body, err
}

// Apply applies a function to inputs.
func (m *Machine) Apply(ctx context.Context, dst [2]stores.RW, fn Function, inputs []Input) (gotfs.Root, error) {
	if len(inputs) != int(fn.Arity) {
		return gotfs.Root{}, fmt.Errorf("function takes %d inputs, have %d", fn.Arity, len(inputs))
	}
	ec := evalCtx{
		Ctx:    ctx,
		Inputs: inputs,
		Dst:    dst,
		Job:    gotjob.New(ctx),
	}
	body, err := m.getBody(ctx, dst[1], fn)
	if err != nil {
		return gotfs.Root{}, err
	}
	return m.evalRoot(&ec, body)
}

type Input struct {
	Stores [2]stores.Reading
	Root   gotfs.Root
}

// evalCtx holds the context for evaluating expressions.
type evalCtx struct {
	Ctx    context.Context
	Dst    [2]stores.RW
	Inputs []Input
	Job    gotjob.Ctx
}

// eval evaluates an expression.
func (m *Machine) eval(ectx *evalCtx, expr *Expr) (Value, error) {
	ctx := ectx.Ctx
	args := expr.Args

	switch expr.Op {
	case OpCode_Nat:
		return expr.Data, nil
	case OpCode_Data:
		return expr.Data, nil
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
		root, err := m.evalRoot(ectx, args[0])
		if err != nil {
			return nil, err
		}
		span, err := m.evalSpan(ectx, args[1])
		if err != nil {
			return nil, err
		}
		seg := gotfs.Segment{Span: span, Contents: root.ToGotKV()}
		return &Value_Segment{seg}, nil
	case OpCode_ShiftOut:
		panic("ShiftOut not yet implemented")
	case OpCode_ShiftIn:
		panic("ShiftIn not yet implemented")
	case OpCode_PICK:
		root, err := m.evalRoot(ectx, args[0])
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
		base, err := m.evalRoot(ectx, args[0])
		if err != nil {
			return nil, err
		}
		path, err := m.evalPath(ectx, args[1])
		if err != nil {
			return nil, err
		}
		mount, err := m.evalRoot(ectx, args[2])
		if err != nil {
			return nil, err
		}
		result, err := m.gotfs.Graft(ectx.Ctx, ectx.Dst, base, path, mount)
		if err != nil {
			return nil, err
		}
		return &Value_Root{Root: *result}, nil
	case OpCode_MKDIRALL:
		root, err := m.evalRoot(ectx, args[0])
		if err != nil {
			return nil, err
		}
		path, err := m.evalPath(ectx, args[1])
		if err != nil {
			return nil, err
		}
		_, err = m.evalFileMode(ectx, args[2])
		if err != nil {
			return nil, err
		}
		result, err := m.gotfs.MkdirAll(ctx, ectx.Dst[1], root, path)
		if err != nil {
			return nil, err
		}
		return &Value_Root{Root: *result}, nil
	case OpCode_CONCAT:
		segs, err := m.flattenConcat(ectx, nil, expr)
		if err != nil {
			return nil, err
		}
		seg, err := m.gotfs.Concat(ctx, ectx.Dst, slices.Values(segs))
		if err != nil {
			return nil, err
		}
		return &Value_Segment{seg}, nil
	case OpCode_PROMOTE:
		seg, err := m.evalSegment(ectx, args[0])
		if err != nil {
			return nil, err
		}
		root := gotfs.Root{Ref: seg.Contents.Ref, Depth: seg.Contents.Depth}
		if err := m.gotfs.Check(ctx, ectx.Dst[0], root, func(ref gdat.Ref) error { return nil }); err != nil {
			return nil, err
		}
		return &Value_Root{Root: root}, nil

	default:
		return nil, fmt.Errorf("unrecognized op %v", expr.Op)
	}
}

// evalRoot calls eval but errors if the result is not a root.
func (m *Machine) evalRoot(ectx *evalCtx, expr *Expr) (gotfs.Root, error) {
	val, err := m.eval(ectx, expr)
	if err != nil {
		return gotfs.Root{}, err
	}
	valroot, ok := val.(*Value_Root)
	if !ok {
		return gotfs.Root{}, fmt.Errorf("expression did not evaluate to a root, got %T", val)
	}
	return valroot.Root, nil
}

func (m *Machine) evalInt(ectx *evalCtx, x *Expr) (int32, error) {
	val, err := m.eval(ectx, x)
	if err != nil {
		return 0, err
	}
	idx, ok := val.(Value_Nat)
	if !ok {
		return 0, fmt.Errorf("expected int, got %T", val)
	}
	return int32(idx), nil
}

func (m *Machine) evalSegment(ectx *evalCtx, expr *Expr) (gotfs.Segment, error) {
	val, err := m.eval(ectx, expr)
	if err != nil {
		return gotfs.Segment{}, err
	}
	v, ok := val.(*Value_Segment)
	if !ok {
		return gotfs.Segment{}, fmt.Errorf("expected segment, got %T", val)
	}
	return v.Segment, nil
}

func (m *Machine) evalSpan(ectx *evalCtx, expr *Expr) (gotfs.Span, error) {
	val, err := m.eval(ectx, expr)
	if err != nil {
		return gotfs.Span{}, err
	}
	v, ok := val.(*Value_Span)
	if !ok {
		return gotfs.Span{}, fmt.Errorf("expected span, got %T", val)
	}
	return gotfs.Span(v.Span), nil
}

func (m *Machine) evalFileMode(ectx *evalCtx, expr *Expr) (os.FileMode, error) {
	val, err := m.eval(ectx, expr)
	if err != nil {
		return 0, err
	}
	v, ok := val.(Value_FileMode)
	if !ok {
		return 0, fmt.Errorf("expected filemode, got %T", val)
	}
	return os.FileMode(v), nil
}

func (m *Machine) evalPath(ectx *evalCtx, expr *Expr) (string, error) {
	val, err := m.eval(ectx, expr)
	if err != nil {
		return "", err
	}
	v, ok := val.(*Value_Path)
	if !ok {
		return "", fmt.Errorf("expected path, got %T", val)
	}
	return string(*v), nil
}

func (m *Machine) flattenConcat(ectx *evalCtx, out []gotfs.Segment, expr *Expr) ([]gotfs.Segment, error) {
	if expr.Op == OpCode_CONCAT {
		var err error
		out, err = m.flattenConcat(ectx, out, expr.Args[0])
		if err != nil {
			return nil, err
		}
		out, err = m.flattenConcat(ectx, out, expr.Args[1])
		if err != nil {
			return nil, err
		}
	} else {
		seg, err := m.evalSegment(ectx, expr)
		if err != nil {
			return nil, err
		}
		out = append(out, seg)
	}
	return out, nil
}
