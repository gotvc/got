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
	"go.brendoncarroll.net/exp/slices2"
)

// Machine holds configuration for operating on GotFS filesystems.
type Machine struct {
	gotfs *gotfs.Machine
	gdat  *gdat.Machine
}

func New(fsmach *gotfs.Machine) Machine {
	return Machine{gotfs: fsmach, gdat: gdat.NewMachine(gdat.Params{})}
}

// Apply applies a function to inputs.
func (m *Machine) Apply(ctx context.Context, dst gotfs.RW, fn Function, inputs []Input) (gotfs.Root, error) {
	if len(inputs) != int(fn.Arity) {
		return gotfs.Root{}, fmt.Errorf("function takes %d inputs, have %d", fn.Arity, len(inputs))
	}
	body, err := m.loadFunction(ctx, dst.Metadata, fn.Ref)
	if err != nil {
		return gotfs.Root{}, err
	}
	src := union(slices2.Map(inputs, func(in Input) gotfs.RO {
		return in.Stores
	})...)
	ec := evalCtx{
		Ctx:    ctx,
		Inputs: inputs,
		Src:    src,
		Dst:    dst,
		Job:    gotjob.New(ctx),
		Fn:     body,
	}
	root, err := m.evalRoot(&ec, body.Output())
	if err != nil {
		return gotfs.Root{}, err
	}
	return root.Root, nil
}

type Input struct {
	Stores gotfs.RO
	Root   gotfs.Root
}

// evalCtx holds the context for evaluating expressions.
type evalCtx struct {
	Ctx    context.Context
	Dst    gotfs.RW
	Inputs []Input
	Src    gotfs.RO
	Job    gotjob.Ctx
	Fn     fnBody
}

// eval evaluates an expression.
func (m *Machine) eval(ectx *evalCtx, expr Vertex) (Value, error) {
	ctx := ectx.Ctx
	oc := ectx.Fn.Op(expr)
	args := ectx.Fn.Args(expr)

	if OpCode_Nat == oc&0xff00_0000 {
		ix := ectx.Fn.at(expr)
		return Value_Nat(ix & 0x00ff_ffff), nil
	}

	switch oc {
	case OpCode_Data:
		nat, err := m.evalNat(ectx, args[0])
		if err != nil {
			return nil, err
		}
		idx := uint32(nat)
		if idx >= ectx.Fn.DataLen() {
			return nil, fmt.Errorf("data index %d out of bounds (have %d)", idx, ectx.Fn.DataLen())
		}
		return ectx.Fn.Data(idx), nil
	case OpCode_Input:
		idx, err := m.evalNat(ectx, args[0])
		if err != nil {
			return nil, err
		}
		if len(ectx.Inputs) <= int(idx) {
			return nil, fmt.Errorf("input index out of bounds %d", idx)
		}
		return &Value_Root{Root: ectx.Inputs[idx].Root}, nil
	case OpCode_SELECT:
		rootVal, err := m.evalRoot(ectx, args[0])
		if err != nil {
			return nil, err
		}
		span, err := m.evalSpan(ectx, args[1])
		if err != nil {
			return nil, err
		}
		seg := gotfs.Segment{Span: span, Contents: rootVal.Root.ToGotKV()}
		return &Value_Segment{seg}, nil
	case OpCode_ShiftOut:
		panic("ShiftOut not yet implemented")
	case OpCode_ShiftIn:
		panic("ShiftIn not yet implemented")
	case OpCode_PICK:
		rootVal, err := m.evalRoot(ectx, args[0])
		if err != nil {
			return nil, err
		}
		path, err := m.evalPath(ectx, args[1])
		if err != nil {
			return nil, err
		}
		ss := mkRW(ectx.Src, ectx.Dst)
		result, err := m.gotfs.Pick(ectx.Ctx, ss.Metadata, rootVal.Root, path)
		if err != nil {
			return nil, err
		}
		return &Value_Root{Root: *result}, nil
	case OpCode_PLACE:
		baseVal, err := m.evalRoot(ectx, args[0])
		if err != nil {
			return nil, err
		}
		path, err := m.evalPath(ectx, args[1])
		if err != nil {
			return nil, err
		}
		mountVal, err := m.evalRoot(ectx, args[2])
		if err != nil {
			return nil, err
		}
		ss := mkRW(ectx.Src, ectx.Dst)
		result, err := m.gotfs.Graft(ectx.Ctx, ss, baseVal.Root, path, mountVal.Root)
		if err != nil {
			return nil, err
		}
		return &Value_Root{Root: *result}, nil
	case OpCode_MKDIRALL:
		rootVal, err := m.evalRoot(ectx, args[0])
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
		ss := mkRW(ectx.Src, ectx.Dst)
		result, err := m.gotfs.MkdirAll(ctx, ss.Metadata, rootVal.Root, path)
		if err != nil {
			return nil, err
		}
		return &Value_Root{Root: *result}, nil
	case OpCode_CONCAT:
		segs, err := m.flattenConcat(ectx, nil, expr)
		if err != nil {
			return nil, err
		}
		ss := mkRW(ectx.Src, ectx.Dst)
		seg, err := m.gotfs.Concat(ctx, ss, slices.Values(segs))
		if err != nil {
			return nil, err
		}
		return &Value_Segment{seg}, nil
	case OpCode_PROMOTE:
		segVal, err := m.evalSegment(ectx, args[0])
		if err != nil {
			return nil, err
		}
		seg := segVal.Segment
		root := gotfs.Root{Ref: seg.Contents.Ref, Depth: seg.Contents.Depth}
		ss := mkRW(ectx.Src, ectx.Dst)
		if err := m.gotfs.Check(ctx, ss.Metadata, root, func(ref gdat.Ref) error { return nil }); err != nil {
			return nil, err
		}
		return &Value_Root{Root: root}, nil

	default:
		return nil, fmt.Errorf("unrecognized op %v", oc)
	}
}

// evalRoot calls eval but errors if the result is not a root.
func (m *Machine) evalRoot(ectx *evalCtx, expr Vertex) (Value_Root, error) {
	val, err := m.eval(ectx, expr)
	if err != nil {
		return Value_Root{}, err
	}
	valroot, ok := val.(*Value_Root)
	if !ok {
		return Value_Root{}, fmt.Errorf("expression did not evaluate to a root, got %T", val)
	}
	return *valroot, nil
}

func (m *Machine) evalNat(ectx *evalCtx, x Vertex) (Value_Nat, error) {
	val, err := m.eval(ectx, x)
	if err != nil {
		return 0, err
	}
	nat, ok := val.(Value_Nat)
	if !ok {
		return 0, fmt.Errorf("expected int, got %T", val)
	}
	return nat, nil
}

func (m *Machine) evalSegment(ectx *evalCtx, expr Vertex) (Value_Segment, error) {
	val, err := m.eval(ectx, expr)
	if err != nil {
		return Value_Segment{}, err
	}
	v, ok := val.(*Value_Segment)
	if !ok {
		return Value_Segment{}, fmt.Errorf("expected segment, got %T", val)
	}
	return *v, nil
}

func (m *Machine) evalSpan(ectx *evalCtx, expr Vertex) (gotfs.Span, error) {
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

func (m *Machine) evalFileMode(ectx *evalCtx, expr Vertex) (os.FileMode, error) {
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

func (m *Machine) evalPath(ectx *evalCtx, expr Vertex) (string, error) {
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

func (m *Machine) flattenConcat(ectx *evalCtx, out []gotfs.Segment, expr Vertex) ([]gotfs.Segment, error) {
	op := ectx.Fn.Op(expr)
	args := ectx.Fn.Args(expr)
	if op == OpCode_CONCAT {
		var err error
		out, err = m.flattenConcat(ectx, out, args[0])
		if err != nil {
			return nil, err
		}
		out, err = m.flattenConcat(ectx, out, args[1])
		if err != nil {
			return nil, err
		}
	} else {
		segVal, err := m.evalSegment(ectx, expr)
		if err != nil {
			return nil, err
		}
		out = append(out, segVal.Segment)
	}
	return out, nil
}

func mkRW(ro gotfs.RO, rw gotfs.RW) gotfs.RW {
	return gotfs.RW{
		Data:     stores.NewOverlay(ro.Data, rw.Data),
		Metadata: stores.NewOverlay(ro.Metadata, rw.Metadata),
	}
}

func union(ros ...gotfs.RO) gotfs.RO {
	return gotfs.RO{
		Data: stores.Union(slices2.Map(ros, func(ss gotfs.RO) stores.Reading {
			return ss.Data
		})),
		Metadata: stores.Union(slices2.Map(ros, func(ss gotfs.RO) stores.Reading {
			return ss.Metadata
		})),
	}
}
