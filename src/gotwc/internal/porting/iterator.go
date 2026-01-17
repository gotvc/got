package porting

import (
	"context"
	"io/fs"
	"os"
	"time"

	"go.brendoncarroll.net/exp/streams"
)

type DirEnt struct {
	Name       string
	Mode       fs.FileMode
	ModifiedAt time.Time
}

var _ streams.Iterator[DirEnt] = &DirEntIterator{}

type DirEntIterator struct {
	root   *os.Root
	filter func(p string) bool

	paths *streams.Slice[DirEnt]
}

func NewDirEntIterator(root *os.Root, filter func(p string) bool) *DirEntIterator {
	return &DirEntIterator{root: root, filter: filter}
}

func (it *DirEntIterator) Next(ctx context.Context, dst *DirEnt) error {
	if it.paths == nil {
		des, err := fs.ReadDir(it.root.FS(), "")
		if err != nil {
			return err
		}
		dirents := make([]DirEnt, len(des))
		for i := range dirents {
			if !it.filter(des[i].Name()) {
				continue
			}
			finfo, err := des[i].Info()
			if err != nil {
				return err
			}
			dirents[i] = DirEnt{
				Name:       des[i].Name(),
				Mode:       finfo.Mode(),
				ModifiedAt: finfo.ModTime(),
			}
		}
		it.paths = streams.NewSlice(dirents, nil)
	}
	return it.paths.Next(ctx, dst)
}
