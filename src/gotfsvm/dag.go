package gotfsvm

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/internal/stores"
)

// I is a single instruction, it represents a node in a computation DAG.
type I uint32

// IWriter writes an instruction stream.
type IWriter struct {
	mach *Machine

	// out is the buffer being written to
	out []byte
	// cache is the index that each *Expr was written to, this allows a DAG instead of
	// just a tree
	cache map[*Expr]uint32
	// data is the per-function data table of constant values
	data []Value
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

	// special cases
	switch x.Op {
	case OpCode_Data:
		// if there is data, then we need to take it and
		// add it to the data table, and replace the first arg with the index
		dataIdx := Value_Nat(len(w.data))
		w.data = append(w.data, x.Data)
		x.Args[0] = Literal(dataIdx)
	}

	arr := x.Op.Arity()
	// args holds the args offsets
	var args [3]uint32
	for i := range args[:arr] {
		var err error
		args[i], err = w.WriteExpr(x.Args[i])
		if err != nil {
			return 0, err
		}
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
		payload |= I(x.Op) & 0x3f00_0000
		if x.Op == OpCode_Nat {
			payload |= I(x.Data.(Value_Nat) & 0x00ff_ffff)
		}
	case 1:
		// 14 bits are for the opcode
		payload |= I(x.Op)
		// low 16 bits are for the arg offset
		payload |= I(args[0]) & 0xffff
	case 2:
		// 14 bits are for the opcode
		payload |= I(x.Op)
		// each arg gets 8 bits of offset
		for i := range args[:arr] {
			if args[i] > 255 {
				return 0, fmt.Errorf("relative offset too large %d", args[i])
			}
			payload |= I(args[i]&0xff) << (i * 8)
		}
	case 3:
		// 6 bits are for the opcode
		payload |= I(x.Op) & 0x3f00_0000
		// each arg gets 8 bits of offset
		for i := range args[:arr] {
			if args[i] > 255 {
				return 0, fmt.Errorf("relative offset too large %d", args[i])
			}
			payload |= I(args[i]&0xff) << (i * 8)
		}
	}
	// set the arity
	payload |= I(arr) << 30

	w.out = binary.LittleEndian.AppendUint32(w.out, uint32(payload))
	w.cache[x] = offset
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
	for _, val := range w.data {
		tv, err := marshalValue(val)
		if err != nil {
			return gdat.Ref{}, err
		}
		if err := enc.Encode(tv); err != nil {
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

func parseFunctionBody(ctx context.Context, gm *gdat.Machine, s stores.Reading, data []byte) (*Expr, error) {
	if len(data) < gdat.RefSize {
		return nil, fmt.Errorf("data too short: %d", len(data))
	}

	// Fetch the literals from the ref in the first 64 bytes.
	var ref gdat.Ref
	if err := ref.Unmarshal(data[:gdat.RefSize]); err != nil {
		return nil, fmt.Errorf("parsing constants ref: %w", err)
	}
	var vals []Value
	if err := gm.GetF(ctx, s, ref, func(vdata []byte) error {
		dec := json.NewDecoder(bytes.NewReader(vdata))
		for dec.More() {
			var tv taggedValue
			if err := dec.Decode(&tv); err != nil {
				return err
			}
			v, err := unmarshalValue(&tv)
			if err != nil {
				return err
			}
			vals = append(vals, v)
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("fetching constants: %w", err)
	}

	instData := data[gdat.RefSize:]
	if len(instData)%4 != 0 {
		return nil, fmt.Errorf("instruction data not aligned: %d", len(instData))
	}
	n := len(instData) / 4
	if n == 0 {
		return nil, fmt.Errorf("no instructions")
	}

	// Decode each instruction into an Expr, storing them by index.
	exprs := make([]*Expr, n)
	for i := range n {
		raw := binary.LittleEndian.Uint32(instData[i*4 : (i+1)*4])
		payload := I(raw)
		arity := int(payload >> 30)

		var op OpCode
		var args [3]uint32

		switch arity {
		case 0:
			// opcode in bits 24-29, nat value in bits 0-23
			op = OpCode(payload) & 0x3f00_0000
			op |= OpCode(arity) << 30
		case 1:
			// opcode in bits 16-31 (including arity), arg offset in bits 0-15
			op = OpCode(payload) & 0xffff_0000
			args[0] = uint32(payload) & 0xffff
		case 2:
			// opcode in bits 16-31 (including arity), args in bits 0-15
			op = OpCode(payload) & 0xffff_0000
			args[0] = uint32(payload) & 0xff
			args[1] = (uint32(payload) >> 8) & 0xff
		case 3:
			// opcode in bits 24-31 (including arity), args in bits 0-23
			op = OpCode(payload) & 0xff00_0000
			args[0] = uint32(payload) & 0xff
			args[1] = (uint32(payload) >> 8) & 0xff
			args[2] = (uint32(payload) >> 16) & 0xff
		}

		expr := &Expr{Op: op}

		if arity == 0 && op == OpCode_Nat {
			natVal := Value_Nat(uint32(payload) & 0x00ff_ffff)
			expr.Data = natVal
		}

		// Resolve relative arg offsets back to absolute indices.
		for j := range arity {
			absIdx := int(i) - int(args[j]) - 1
			if absIdx < 0 || absIdx >= i {
				return nil, fmt.Errorf("instruction %d: arg %d offset %d out of bounds", i, j, args[j])
			}
			expr.Args[j] = exprs[absIdx]
		}

		if op == OpCode_Data {
			// The first arg is a Nat holding the index into the data table.
			natExpr := expr.Args[0]
			if natExpr == nil || natExpr.Op != OpCode_Nat {
				return nil, fmt.Errorf("instruction %d: Data op missing Nat arg", i)
			}
			dataIdx := int(natExpr.Data.(Value_Nat))
			if dataIdx >= len(vals) {
				return nil, fmt.Errorf("data index %d out of bounds (have %d)", dataIdx, len(vals))
			}
			expr.Data = vals[dataIdx]
		}

		exprs[i] = expr
	}

	// The last instruction is the root of the expression.
	return exprs[n-1], nil
}
