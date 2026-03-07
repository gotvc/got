package gotfsvm

import (
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
type Value_Segment gotfs.Segment

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
