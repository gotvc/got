package gotfsvm

import (
	"fmt"
	"os"

	"github.com/gotvc/got/src/gotfs"
	"go.brendoncarroll.net/exp/sbe"
)

type TypeCode uint8

const (
	Type_UNKNOWN = iota

	Type_Nat
	Type_Root
	Type_Segment
	Type_Span
	Type_Info
	Type_Extent
	Type_Path
	Type_FileMode
)

type Value interface {
	isValue()
}

// marshalValue appends the TypeCode for v to out, and then marshals v.
func marshalValue(x Value, out []byte) []byte {
	if x == nil {
		return out
	}
	switch x := x.(type) {
	case *Value_Root:
		out = append(out, Type_Root)
		out = x.Root.Marshal(out)
	case *Value_Segment:
		out = append(out, Type_Segment)
		out = x.Segment.Marshal(out)
	case *Value_Extent:
		out = append(out, Type_Extent)
		data, err := x.Extent.MarshalBinary()
		if err != nil {
			panic(err)
		}
		out = append(out, data...)
	case *Value_Info:
		out = append(out, Type_Info)
		out = x.Info.Marshal(out)
	case Value_Nat:
		out = append(out, Type_Nat)
		out = sbe.AppendUint32(out, uint32(x))
	case *Value_Span:
		out = append(out, Type_Span)
		out = x.Span.Marshal(out)
	case *Value_Path:
		out = append(out, Type_Path)
		out = append(out, *x...)
	case Value_FileMode:
		out = append(out, Type_FileMode)
		out = sbe.AppendUint32(out, uint32(x))
	default:
		panic(x)
	}
	return out
}

func parseValue(data []byte) (Value, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("too short to be value")
	}
	ty := TypeCode(data[0])
	data = data[1:]
	switch ty {
	case Type_Root:
		var r gotfs.Root
		if err := r.Unmarshal(data); err != nil {
			return nil, err
		}
		return &Value_Root{Root: r}, nil
	case Type_Segment:
		var s gotfs.Segment
		if err := s.Unmarshal(data); err != nil {
			return nil, err
		}
		return &Value_Segment{Segment: s}, nil
	case Type_Extent:
		var e gotfs.Extent
		if err := e.UnmarshalBinary(data); err != nil {
			return nil, err
		}
		return &Value_Extent{Extent: e}, nil
	case Type_Info:
		var info gotfs.Info
		if err := info.Unmarshal(data); err != nil {
			return nil, err
		}
		return &Value_Info{Info: info}, nil
	case Type_Nat:
		v, _, err := sbe.ReadUint32(data)
		if err != nil {
			return nil, err
		}
		return Value_Nat(v), nil
	case Type_Span:
		var s gotfs.Span
		if err := s.Unmarshal(data); err != nil {
			return nil, err
		}
		return &Value_Span{Span: s}, nil
	case Type_Path:
		p := Value_Path(data)
		return &p, nil
	case Type_FileMode:
		v, _, err := sbe.ReadUint32(data)
		if err != nil {
			return nil, err
		}
		return Value_FileMode(v), nil
	default:
		return nil, fmt.Errorf("cannot parse value of unknown type %v", ty)
	}
}

// Value_Root is the root of a GotFS filesystem
type Value_Root struct {
	Root gotfs.Root

	stores gotfs.RO
}

func (r *Value_Root) isValue() {}

// Value_Segment is a segment of a filesystem, not a valid filesystem on it's own.
type Value_Segment struct {
	Segment gotfs.Segment

	stores gotfs.RO
}

func (r *Value_Segment) isValue() {}

// Value_Extent is a reference to data
type Value_Extent struct {
	Extent gotfs.Extent

	stores gotfs.RO
}

func (r *Value_Extent) isValue() {}

type Value_Info struct {
	Info gotfs.Info
}

func (r *Value_Info) isValue() {}

type Value_Nat uint32

func (r Value_Nat) isValue() {}

// Value_Span is a span within a filesystem
type Value_Span struct {
	Span gotfs.Span
}

func (r *Value_Span) isValue() {}

// Value_Path is a path within a filesystem
type Value_Path string

func (r *Value_Path) isValue() {}

// Value_FileMode is a file mode
type Value_FileMode os.FileMode

func (r Value_FileMode) isValue() {}
