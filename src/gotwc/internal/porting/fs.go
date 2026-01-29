package porting

import (
	"path"
	"strings"

	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/state/posixfs"
	"go.brendoncarroll.net/tai64"
	"golang.org/x/exp/slices"
)

// NewFSInfoIter iterates over all the tracked paths in the filesystem.
func NewFSInfoIter(fsys posixfs.FS) streams.Iterator[FileInfo] {
	seq := func(yield func(FileInfo, error) bool) {
		var walk func(string) bool
		walk = func(p string) bool {
			finfo, err := fsys.Stat(p)
			if err != nil {
				return false
			}
			fi := FileInfo{
				Path: p,

				ModifiedAt: tai64.FromGoTime(finfo.ModTime()),
				Mode:       finfo.Mode(),
				Size:       finfo.Size(),
			}
			// emit the directory before its children.
			if p != "" {
				if !yield(fi, nil) {
					return false
				}
			}
			if finfo.IsDir() {
				dirents, err := posixfs.ReadDir(fsys, p)
				if err != nil {
					return false
				}
				slices.SortFunc(dirents, func(a, b posixfs.DirEnt) int {
					return strings.Compare(a.Name, b.Name)
				})
				for _, dirent := range dirents {
					p2 := path.Join(p, dirent.Name)
					if !walk(p2) {
						return false
					}
				}
			}
			return true
		}
		walk("")
	}
	return streams.NewSeqErr(seq)
}
