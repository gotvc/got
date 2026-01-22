package gotwc

import (
	"io/fs"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotwc/internal/porting"
	"go.brendoncarroll.net/exp/streams"
)

// File is information about a file
type File struct {
	Path    string
	Mode    fs.FileMode
	Extents []gotfs.Extent
}

// DirtyIter iterates over the files in the working copy that are dirty
// and emits a path for each dirty file.
type DirtyIter struct {
	fsiter streams.Peekable[porting.FileInfo]
}

func newDirtyIter(base streams.Iterator[File]) *DirtyIter {
	return &DirtyIter{}
}

// func (it *DirtyIter) Next(ctx context.Context, dst []string) error {
// 	for i := range dst {
// 		finfo, err := streams.Peek(ctx, it.fsiter)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }
