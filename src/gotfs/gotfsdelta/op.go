package gotfsdelta

import (
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv"
)

// Op is an operation on the filesystem
// It is a sum type, only one field will be non-nil.
type Op struct {
	// PutFile means the info and content were both changed.
	PutFile    *PutFileOp
	PutContent *PutContentOp
	PutInfo    *PutInfoOp
	// Delete is a delete on a path in the filesystem.
	Delete *DeleteOp
}

type PutFileOp struct {
	Path string
}

// PutContentOp changes extents only, within a single path.
type PutContentOp struct {
	Path    string
	StartAt uint64
	Extents []gotfs.Extent
}

func isPutContentOp(seg gotkv.Segment) bool {
	var k gotfs.Key
	if err := k.Unmarshal(seg.Span.Begin); err != nil {
		return false
	}
	return !k.IsInfo()
}

type DeleteOp string

func isDelete(seg gotkv.Segment) bool {
	return seg.IsDelete()
}

type PutOp struct {
	Path string
	Root Root
}

func isPutInfo(seg gotkv.Segment) bool {
	return seg.Contents.Count == 1
}

type PutInfoOp struct {
	Path string
	Info gotfs.Info
}
