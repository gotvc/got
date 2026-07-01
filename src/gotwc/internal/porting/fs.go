package porting

import (
	"context"
	"encoding/json"
	"io/fs"
	"iter"
	"path"
	"strings"

	"github.com/gotvc/got/src/gotfs"
	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/state/posixfs"
	"go.brendoncarroll.net/tai64"
	"golang.org/x/exp/slices"
)

// FileInfo is a struct version of io/fs.FileInfo
type FileInfo struct {
	ModifiedAt tai64.TAI64N
	Mode       fs.FileMode
	Size       int64
}

func (fi *FileInfo) Marshal(out []byte) []byte {
	data, _ := json.Marshal(fi)
	return append(out, data...)
}

func (fi *FileInfo) Unmarshal(data []byte) error {
	return nil
}

type FilePair struct {
	Path string
	Info FileInfo
}

// GetExtents returns the extents for a file.
// If the file has not been modified, then
func GetExtents(ctx context.Context, db *DB, fsys posixfs.FS, p string) iter.Seq2[gotfs.Extent, error] {
	return func(yield func(gotfs.Extent, error) bool) {

	}
}

// NewFSInfoIter iterates over all the tracked paths in the filesystem.
func NewFSInfoIter(fsys posixfs.FS, base string) streams.Iterator[InfoEntry] {
	seq := func(yield func(InfoEntry, error) bool) {
		var walk func(string) bool
		walk = func(p string) bool {
			finfo, err := fsys.Stat(p)
			if err != nil {
				return false
			}
			fi := InfoEntry{
				Path: p,
				Info: FileInfo{
					ModifiedAt: tai64.FromGoTime(finfo.ModTime()),
					Mode:       finfo.Mode(),
					Size:       finfo.Size(),
				},
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
		walk(base)
	}
	return streams.NewSeqErr(seq)
}
