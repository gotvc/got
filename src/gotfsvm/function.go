package gotfsvm

import (
	"context"
	"encoding/binary"
	"fmt"
	"io/fs"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/sbe"
	"go.brendoncarroll.net/exp/streams"
)

const (
	dataTabHeaderSize = gdat.RefSize + 4
)

var dataTabParams = gotkv.Params{MaxSize: 1 << 16, MeanSize: 1 << 13}

// Function maps a []gotfs.Root to a gotfs.Root
// Function is a root expression, and the number of inputs it takes.
type Function struct {
	Ref gdat.Ref
	// Arity is the number of inputs the function takes
	Arity uint32
}

func (fn Function) Marshal(out []byte) []byte {
	out = append(out, fn.Ref.Marshal()...)
	out = sbe.AppendUint32(out, fn.Arity)
	return out
}

func (fn *Function) Unmarshal(data []byte) error {
	refData, data, err := sbe.ReadN(data, gdat.RefSize)
	if err != nil {
		return err
	}
	if err := fn.Ref.Unmarshal(refData); err != nil {
		return err
	}
	arity, _, err := sbe.ReadUint32(data)
	if err != nil {
		return err
	}
	fn.Arity = arity
	return nil
}

// NewFunction creates a new function from an expression.
func (m *Machine) NewFunction(ctx context.Context, s stores.RW, fn func(*FnBuilder) (Expr[gotfs.Root], error)) (Function, error) {
	fb := m.NewBuilder(s)
	out, err := fn(&fb)
	if err != nil {
		return Function{}, err
	}
	fb.SetOutput(out)
	// TODO: truncate to
	return fb.Flush(ctx)
}

type FnBuilder struct {
	mach  *Machine
	gotkv gotkv.Machine
	s     stores.RW

	fc        fnBody
	numInputs uint32
}

func (m *Machine) NewBuilder(s stores.RW) FnBuilder {
	return FnBuilder{
		mach:  m,
		gotkv: gotkv.NewMachine(dataTabParams),
		s:     s,
	}
}

func (fb *FnBuilder) SetOutput(x Expr[gotfs.Root]) {
	if x.i < Vertex(len(fb.fc.dag)-1) {
		// TODO: passthrough OP to reference x.i
		panic("TODO")
	}
}

// Flush creates a new Function and returns it.
// The data table is written, and then it and the program make up the rest of the function body.
// The function body is posted.
func (fb *FnBuilder) Flush(ctx context.Context) (Function, error) {
	out := make([]byte, 0, dataTabHeaderSize+len(fb.fc.dag)*ISize)

	// write the data table to gotkv.
	kvroot, err := func() (gotkv.Root, error) {
		b := fb.gotkv.NewBuilder(fb.s)
		for i, val := range fb.fc.data {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			val := marshalValue(val, nil)
			if err := b.Put(ctx, key, val); err != nil {
				return gotkv.Root{}, err
			}
		}
		return b.Finish(ctx)
	}()
	if err != nil {
		return Function{}, err
	}
	// append the gdat.Ref and depth, but not the First key (that is always uint32(0))
	out = kvroot.Marshal(out)
	out = out[:dataTabHeaderSize]
	copy(out[len(out)-3:], []byte{0, 0, 0}) // 0 the last 4, we only want the depth

	// append the DAG
	out = fb.fc.marshalDAG(out)

	// post the whole buffer
	ref, err := fb.mach.gdat.Post(ctx, fb.s, out)
	if err != nil {
		return Function{}, err
	}
	return Function{Arity: fb.numInputs, Ref: ref}, nil
}

// Expr is a specification for a value to be computed.
// The type of the value is known ahead of time.
type Expr[T any] struct {
	i Vertex
}

func (fb *FnBuilder) Nat(val uint32) Expr[uint32] {
	return Expr[uint32]{
		fb.fc.append0(OpCode(OpCode_Nat | (0x00ff_ffff & val))),
	}
}

// Input returns an Expr which will evaluate to the input.
func (fb *FnBuilder) Input(i uint32) Expr[gotfs.Root] {
	fb.numInputs = max(i+1, fb.numInputs)
	n := fb.Nat(i)
	return Expr[gotfs.Root]{fb.fc.append1(OpCode_Input, n.i)}
}

func (fb *FnBuilder) Root(root gotfs.Root) Expr[gotfs.Root] {
	dataIdx := fb.fc.appendData(&Value_Root{Root: root})
	n := fb.Nat(uint32(dataIdx))
	return Expr[gotfs.Root]{fb.fc.append1(OpCode_Data, n.i)}
}

// Segment adds a Segment as data to the function.
func (fb *FnBuilder) Segment(seg gotfs.Segment) Expr[gotfs.Segment] {
	dataIdx := fb.fc.appendData(&Value_Segment{Segment: seg})
	n := fb.Nat(uint32(dataIdx))
	return Expr[gotfs.Segment]{fb.fc.append1(OpCode_Data, n.i)}
}

func (fb *FnBuilder) Path(p string) Expr[string] {
	v := Value_Path(p)
	dataIdx := fb.fc.appendData(&v)
	n := fb.Nat(uint32(dataIdx))
	return Expr[string]{fb.fc.append1(OpCode_Data, n.i)}
}

func (fb *FnBuilder) FileMode(m fs.FileMode) Expr[fs.FileMode] {
	dataIdx := fb.fc.appendData(Value_FileMode(m))
	n := fb.Nat(uint32(dataIdx))
	return Expr[fs.FileMode]{fb.fc.append1(OpCode_Data, n.i)}
}

func (fb *FnBuilder) Span(span gotfs.Span) Expr[gotfs.Span] {
	dataIdx := fb.fc.appendData(&Value_Span{Span: span})
	n := fb.Nat(uint32(dataIdx))
	return Expr[gotfs.Span]{fb.fc.append1(OpCode_Data, n.i)}
}

func (fb *FnBuilder) Promote(x Expr[gotfs.Segment]) Expr[gotfs.Root] {
	return Expr[gotfs.Root]{fb.fc.append1(OpCode_PROMOTE, x.i)}
}

func (fb *FnBuilder) Concat(xs ...Expr[gotfs.Segment]) Expr[gotfs.Segment] {
	switch len(xs) {
	case 0:
		return Expr[gotfs.Segment]{}
	case 1:
		return xs[0]
	case 2:
		v := fb.fc.append2(OpCode_CONCAT, xs[0].i, xs[1].i)
		return Expr[gotfs.Segment]{v}
	default:
		l := fb.Concat(xs[:len(xs)/2]...)
		r := fb.Concat(xs[len(xs)/2:]...)
		return fb.Concat(l, r)
	}
}

func (fb *FnBuilder) ChangesOnBase(base Expr[gotfs.Root], changes []gotfs.Segment) Expr[gotfs.Segment] {
	var exprs []Expr[gotfs.Segment]
	for i := range changes {
		var baseSpan gotkv.Span
		if i > 0 {
			baseSpan.Begin = changes[i-1].Span.End
		}
		baseSpan.End = changes[i].Span.Begin
		exprs = append(exprs, fb.Select(base, baseSpan))
		exprs = append(exprs, fb.Segment(changes[i]))
	}
	if len(exprs) > 0 {
		exprs = append(exprs, fb.Select(base, gotkv.Span{
			Begin: changes[len(changes)-1].Span.End,
			End:   nil,
		}))
	}
	return fb.Concat(exprs...)
}

func (fb *FnBuilder) Select(root Expr[gotfs.Root], span gotkv.Span) Expr[gotfs.Segment] {
	spanV := fb.Span(span)
	return Expr[gotfs.Segment]{fb.fc.append2(OpCode_SELECT, root.i, spanV.i)}
}

func (fb *FnBuilder) MkdirAll(base Expr[gotfs.Root], p string, mode fs.FileMode) Expr[gotfs.Root] {
	pathV := fb.Path(p)
	modeV := fb.FileMode(mode)
	return Expr[gotfs.Root]{fb.fc.append3(OpCode_MKDIRALL, base.i, pathV.i, modeV.i)}
}

// I is a single instruction, it represents a node in a computation DAG.
type I uint32

func (i I) Arity() int {
	return int(i >> 30)
}

func (i I) Op() OpCode {
	switch i.Arity() {
	case 0:
		return OpCode(i)
	case 1:
		// high 16 bits
		return OpCode(i & 0xffff_0000)
	case 2:
		return OpCode(i & 0xffff_0000)
	case 3:
		return OpCode(i & 0xff00_0000)
	default:
		return 0
	}
}

// Args returns relative offsets to the inputs of this step
func (i I) Args() (ret [3]uint32) {
	switch i.Arity() {
	case 1:
		ret[0] = uint32(i) & 0x0000_ffff
	case 2:
		ret[0] = uint32(i) & 0xff
		ret[1] = (uint32(i) >> 8) & 0xff
	case 3:
		ret[0] = uint32(i) & 0xff
		ret[1] = (uint32(i) >> 8) & 0xff
		ret[2] = (uint32(i) >> 16) & 0xff
	default:
	}
	return ret
}

// ISize is the size of an insturction in bytes
const ISize = 4

// fnBody is a function's prog and data loaded into memory.
// Functions have a computation DAG and a data table.
type fnBody struct {
	data []Value
	dag  []I
}

func (m *Machine) loadFunction(ctx context.Context, s stores.RO, ref gdat.Ref) (fnBody, error) {
	var fb fnBody
	if err := m.gdat.GetF(ctx, s, ref, func(data []byte) error {
		if len(data) < dataTabHeaderSize {
			return fmt.Errorf("data too short: %d", len(data))
		}

		// Reconstruct the gotkv root from the header.
		// The header is: Ref (RefSize) + Depth (1 byte) + 3 zero padding bytes.
		var kvroot gotkv.Root
		if err := kvroot.Unmarshal(data[:dataTabHeaderSize]); err != nil {
			return fmt.Errorf("parsing constants ref: %w", err)
		}
		kvroot.First = binary.BigEndian.AppendUint32(nil, 0)

		// Load values from the data table.
		kvmach := gotkv.NewMachine(dataTabParams)
		it := kvmach.NewIterator(s, kvroot, gotkv.TotalSpan())
		if err := streams.ForEach(ctx, it, func(ent gotkv.Entry) error {
			val, err := parseValue(ent.Value)
			if err != nil {
				return err
			}
			fb.data = append(fb.data, val)
			return nil
		}); err != nil {
			return err
		}

		// Parse the DAG from the remaining bytes.
		return fb.unmarshalDAG(data[dataTabHeaderSize:])
	}); err != nil {
		return fnBody{}, err
	}
	return fb, nil
}

// marshalDAG marshals just the DAG portion of the fnBody and appends it to out.
func (fb *fnBody) marshalDAG(out []byte) []byte {
	for _, ix := range fb.dag {
		out = binary.LittleEndian.AppendUint32(out, uint32(ix))
	}
	return out
}

// unmarshalDAG unmarshals the DAG section.
// data must only be for the data, and cannot include the data table
func (fb *fnBody) unmarshalDAG(data []byte) error {
	if len(data)%ISize != 0 {
		return fmt.Errorf("wrong size for DAG %d", len(data))
	}
	fb.dag = fb.dag[:0]
	for i := 0; i < len(data); i += ISize {
		ix := binary.LittleEndian.Uint32(data[i : i+ISize])
		// TODO: validate relOffsets and opcode
		fb.dag = append(fb.dag, I(ix))
	}
	return nil
}

// Vertex is an index into a functions DAG.
type Vertex uint32

func (fc *fnBody) at(i Vertex) I {
	return fc.dag[i]
}

func (fb *fnBody) appendData(val Value) int {
	fb.data = append(fb.data, val)
	return len(fb.data) - 1
}

func (fb *fnBody) append(op OpCode, args [3]Vertex) Vertex {
	arr := op.Arity()
	// convert to relative offsets
	var relOff [3]uint32
	offset := uint32(len(fb.dag))
	for i := range relOff[:arr] {
		relOff[i] = offset - uint32(args[i]) - 1
	}

	var ix I
	switch arr {
	case 0:
		ix = I(op)
	case 1:
		ix = I(op) | I(relOff[0]&0xffff)
	case 2:
		ix = I(op)
		for i := range relOff[:arr] {
			ix |= I(relOff[i]&0xff) << (i * 8)
		}
	case 3:
		ix = I(op) & 0xff00_0000
		for i := range relOff[:arr] {
			ix |= I(relOff[i]&0xff) << (i * 8)
		}
	}
	// set the arity
	ix |= I(arr) << 30

	fb.dag = append(fb.dag, ix)
	return fb.Output()
}

func (fb *fnBody) append0(op OpCode) Vertex {
	return fb.append(op, [3]Vertex{})
}

func (fb *fnBody) append1(op OpCode, a0 Vertex) Vertex {
	return fb.append(op, [3]Vertex{a0, 0, 0})
}

func (fb *fnBody) append2(op OpCode, a0, a1 Vertex) Vertex {
	return fb.append(op, [3]Vertex{a0, a1, 0})
}

func (fb *fnBody) append3(op OpCode, a0, a1, a2 Vertex) Vertex {
	return fb.append(op, [3]Vertex{a0, a1, a2})
}

func (fc *fnBody) Op(i Vertex) OpCode {
	return fc.at(i).Op()
}

// Args are the inputs to this instruction
func (fc *fnBody) Args(v Vertex) [3]Vertex {
	ix := fc.at(v)
	relOffsets := ix.Args()
	var ret [3]Vertex
	for i := range ret[:ix.Arity()] {
		ret[i] = v - Vertex(relOffsets[i]) - 1
	}
	return ret
}

func (fc *fnBody) DataLen() uint32 {
	return uint32(len(fc.data))
}

func (fc *fnBody) Data(i uint32) Value {
	return fc.data[i]
}

func (fc *fnBody) Output() Vertex {
	return Vertex(len(fc.dag) - 1)
}
