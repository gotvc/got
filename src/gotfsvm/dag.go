package gotfsvm

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/internal/stores"
)

// I is a single instruction, it represents a node in a computation DAG.
type I uint32

// marshalFunc marshals the body of a function, and then
// appends the instruction for the expression.
func marshalFunc(out []byte, body *Expr) []byte {
	cache := make(map[*Expr]uint32)
	_, out = marshalExpr(out, cache, 0, body)
	return out
}

// IWriter writes an instruction stream.
type IWriter struct {
	mach  *Machine
	out   []byte
	cache map[*Expr]uint32
	lits  []Value
}

func NewIWriter(mach *Machine) IWriter {
	// the first 64 bytes are a ref to the constants.
	buf := make([]byte, 64)
	return IWriter{
		mach:  mach,
		out:   buf,
		cache: make(map[*Expr]uint32),
	}
}

func (w *IWriter) WriteExpr(x *Expr) (uint32, error) {
	if idx, exists := w.cache[x]; exists {
		return idx, nil
	}
	offset := uint32(w.ILen())

	arr := x.Op.Arity()
	// args holds the args offsets
	var args [3]uint32
	for i := range args[:arr] {
		args[i], w.out = marshalExpr(w.out, w.cache, offset, x.Args[i])
		if args[i] >= offset {
			// nodes were added, so we need to increment nextIdx
			offset = args[i] + 1
		}
	}
	// reassign arg offsets to the relative offset.
	// There will never be a need for zero offset, so shift 1 down to 0, 2 down to 1, etc.
	for i := range args[:arr] {
		args[i] = offset - args[i] - 1
	}

	// create the instruction
	// the arity always takes bits 30 and 31.
	// the opcode and arg offsets take up different amounts of bits depending on the arity.
	var payload I
	switch arr {
	case 0:
	case 1:
		// top 14 bits are for the opcode
		payload |= I(x.Op) << 16
		// low 16 bits are for the arg offset
		payload |= I(args[0]) & 0xff
	case 2:
		// top 14 bits are for the opcode
		payload |= I(x.Op) << 16
		// each arg gets 8 bits of offset
		for i := range args[:arr] {
			payload |= I(args[i]) << (i * 8)
		}
	case 3:
		// top 6 bits are for the opcode
		payload |= I(x.Op) << 24
		// each arg gets 8 bits of offset
		for i := range args[:arr] {
			payload |= I(args[i]) << (i * 8)
		}
	}
	// set the arity
	payload |= I(arr) << 30

	w.out = binary.LittleEndian.AppendUint32(w.out, uint32(payload))
	return offset, nil
}

func (w *IWriter) ILen() int {
	return (len(w.out) - gdat.RefSize) / 4
}

func (w *IWriter) Bytes() []byte {
	return w.out
}

func (w *IWriter) postValues(ctx context.Context, swo stores.Writing) (gdat.Ref, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, val := range w.lits {
		if err := enc.Encode(val); err != nil {
			return gdat.Ref{}, err
		}
	}
	ref, err := w.mach.gdat.Post(ctx, swo, buf.Bytes())
	if err != nil {
		return gdat.Ref{}, err
	}
	return *ref, nil
}

// Flush flushes the instruction stream to swo, and returns the ref.
// This ref can be included in a Function.
func (w *IWriter) Flush(ctx context.Context, swo stores.Writing) (gdat.Ref, error) {
	ref, err := w.postValues(ctx, swo)
	if err != nil {
		return gdat.Ref{}, err
	}
	copy(w.out[:64], ref.Marshal())
	ref2, err := w.mach.gdat.Post(ctx, swo, w.out)
	if err != nil {
		return gdat.Ref{}, err
	}
	return *ref2, nil
}

// marshalExpr appends the expr to out if necessary and returns the index that it was written to
// as well as the new version of out.
// out may not need to be updated if the expression is in the cache
func marshalExpr(out []byte, cache map[*Expr]uint32, offset uint32, x *Expr) (uint32, []byte) {
	arr := x.Op.Arity()
	// args holds the args offsets
	var args [3]uint32
	for i := range args[:arr] {
		args[i], out = marshalExpr(out, cache, offset, x.Args[i])
		if args[i] >= offset {
			// nodes were added, so we need to increment nextIdx
			offset = args[i] + 1
		}
	}
	// at this point offset will not need to change.
	// reassign arg offsets to the relative offset.
	// There will never be a need for zero offset, so shift 1 down to 0, 2 down to 1, etc.
	for i := range args[:arr] {
		args[i] = offset - args[i] - 1
	}

	var payload I
	switch arr {
	case 0:
	case 1:
		// low 16 bits are for the offset

	case 2:
	case 3:
	}

	payload |= I(arr) << 30
	out = binary.LittleEndian.AppendUint32(out, uint32(payload))

	cache[x] = offset
	return offset, out
}

func parseFunction(data []byte) (Function, error) {
	return Function{}, nil
}
