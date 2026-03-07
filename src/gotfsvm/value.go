package gotfsvm

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/gotvc/got/src/gotfs"
)

type Value interface {
	isValue()
}

// Value_Root is the root of a GotFS filesystem
type Value_Root struct {
	Root gotfs.Root
}

func (r *Value_Root) isValue() {}

// Value_Segment is a segment of a filesystem, not a valid filesystem on it's own.
type Value_Segment struct {
	Segment gotfs.Segment
}

func (r *Value_Segment) isValue() {}

// Value_Extent is a reference to data
type Value_Extent struct {
	Extent gotfs.Extent
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

type taggedValue struct {
	Type string          `json:"t"`
	Data json.RawMessage `json:"d"`
}

func marshalValue(v Value) (*taggedValue, error) {
	if v == nil {
		return nil, nil
	}
	var typeName string
	switch v.(type) {
	case *Value_Root:
		typeName = "root"
	case *Value_Segment:
		typeName = "segment"
	case *Value_Extent:
		typeName = "extent"
	case *Value_Info:
		typeName = "info"
	case Value_Nat:
		typeName = "nat"
	case *Value_Span:
		typeName = "span"
	case *Value_Path:
		typeName = "path"
	case Value_FileMode:
		typeName = "filemode"
	default:
		return nil, fmt.Errorf("unknown value type %T", v)
	}
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &taggedValue{Type: typeName, Data: data}, nil
}

func unmarshalValue(tv *taggedValue) (Value, error) {
	if tv == nil {
		return nil, nil
	}
	switch tv.Type {
	case "root":
		var v Value_Root
		return &v, json.Unmarshal(tv.Data, &v)
	case "segment":
		var v Value_Segment
		return &v, json.Unmarshal(tv.Data, &v)
	case "extent":
		var v Value_Extent
		return &v, json.Unmarshal(tv.Data, &v)
	case "info":
		var v Value_Info
		return &v, json.Unmarshal(tv.Data, &v)
	case "nat":
		var v Value_Nat
		err := json.Unmarshal(tv.Data, &v)
		return v, err
	case "span":
		var v Value_Span
		return &v, json.Unmarshal(tv.Data, &v)
	case "path":
		var v Value_Path
		return &v, json.Unmarshal(tv.Data, &v)
	case "filemode":
		var v Value_FileMode
		err := json.Unmarshal(tv.Data, &v)
		return v, err
	default:
		return nil, fmt.Errorf("unknown value type %q", tv.Type)
	}
}
