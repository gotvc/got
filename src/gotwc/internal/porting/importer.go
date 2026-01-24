// package porting deals with importing and exporting to and from gotfs
package porting

import (
	"context"
	"fmt"
	"io"
	"path"
	"runtime"
	"strings"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/metrics"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/units"
	"go.brendoncarroll.net/state/posixfs"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.brendoncarroll.net/tai64"
	"golang.org/x/exp/slices"
)

type Importer struct {
	gotfs  *gotfs.Machine
	db     *DB
	ms, ds stores.RW
}

func NewImporter(fsmach *gotfs.Machine, db *DB, ss [2]stores.RW) *Importer {
	return &Importer{
		gotfs: fsmach,
		db:    db,

		ms: ss[1],
		ds: ss[0],
	}
}

// ImportPath returns gotfs instance containing the content in fsx at p.
// The content will be at the root of the filesystem.
func (pr *Importer) ImportPath(ctx context.Context, fsx posixfs.FS, p string) (*gotfs.Root, error) {
	stat, err := fsx.Stat(p)
	if err != nil {
		return nil, err
	}
	if !stat.Mode().IsDir() {
		return pr.importFile(ctx, fsx, p)
	}
	var changes []gotfs.Segment
	emptyDir, err := createEmptyDir(ctx, pr.gotfs, pr.ms)
	if err != nil {
		return nil, err
	}
	changes = append(changes, gotfs.Segment{
		Span: gotkv.TotalSpan(),
		Contents: gotfs.Expr{
			Root:      *emptyDir,
			AddPrefix: "",
		},
	})
	dirents, err := posixfs.ReadDir(fsx, p)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(dirents, func(a, b posixfs.DirEnt) int {
		return strings.Compare(a.Name, b.Name)
	})
	metrics.SetDenom(ctx, "paths", len(dirents), "paths")
	for _, dirent := range dirents {
		ctx, cf := metrics.Child(ctx, dirent.Name)
		defer cf()
		p2 := path.Join(p, dirent.Name)
		pathRoot, err := pr.ImportPath(ctx, fsx, p2)
		if err != nil {
			return nil, err
		}
		metrics.AddInt(ctx, "paths", 1, "paths")
		changes = append(changes, gotfs.Segment{
			Span: gotfs.SpanForPath(dirent.Name),
			Contents: gotfs.Expr{
				Root:      *pathRoot,
				AddPrefix: dirent.Name,
			},
		})
	}
	return pr.gotfs.Splice(ctx, [2]stores.RW{pr.ds, pr.ms}, changes)
}

func (pr *Importer) ImportFile(ctx context.Context, fsx posixfs.FS, p string) (*gotfs.Root, error) {
	return pr.importFile(ctx, fsx, p)
}

// ImportFile returns a gotfs.Root with the content from the file in fsx at p.
func (pr *Importer) importFile(ctx context.Context, fsx posixfs.FS, p string) (*gotfs.Root, error) {
	finfo, err := stat(fsx, p)
	if !finfo.Mode.IsRegular() {
		return nil, fmt.Errorf("ImportFile called for non-regular file at path %q", p)
	}
	var ent FileInfo
	if ok, err := pr.db.GetInfo(ctx, p, &ent); err != nil {
		return nil, err
	} else if ok && changed(&ent, finfo) {
		logctx.Infof(ctx, "using cache entry for path %q. skipped import", p)
		var root gotfs.Root
		if yes, err := pr.db.GetFSRoot(ctx, p, &root); err != nil {
			return nil, err
		} else if yes {
			return &root, nil
		}
	}
	fileSize := finfo.Size
	metrics.SetDenom(ctx, "data_in", int(fileSize), units.Bytes)
	numWorkers := runtime.GOMAXPROCS(0)
	sizeCutoff := 20 * pr.gotfs.MeanBlobSizeData() * numWorkers
	// fast path for small files
	var root *gotfs.Root
	if fileSize < int64(sizeCutoff) {
		f, err := fsx.OpenFile(p, posixfs.O_RDONLY, 0)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		root, err = pr.gotfs.FileFromReader(ctx, [2]stores.RW{pr.ds, pr.ms}, finfo.Mode, f)
		if err != nil {
			return nil, err
		}
		metrics.AddInt(ctx, "data_in", int(fileSize), units.Bytes)
	} else {
		root, err = importFileConcurrent(ctx, pr.gotfs, pr.ms, pr.ds, fsx, p, numWorkers)
		if err != nil {
			return nil, err
		}
	}
	// need update
	if err := pr.db.PutInfo(ctx, *finfo); err != nil {
		return nil, err
	}
	if err := pr.db.PutFSRoot(ctx, p, finfo.ModifiedAt, *root); err != nil {
		return nil, err
	}
	return root, nil
}

func stat(fsys posixfs.FS, p string) (*FileInfo, error) {
	finfo, err := fsys.Stat(p)
	if err != nil {
		return nil, err
	}
	return &FileInfo{
		Path:       p,
		ModifiedAt: tai64.FromGoTime(finfo.ModTime()),
		Mode:       finfo.Mode(),
		Size:       finfo.Size(),
	}, nil
}

func importFileConcurrent(ctx context.Context, fsag *gotfs.Machine, ms, ds stores.RW, fsx posixfs.FS, p string, numWorkers int) (*gotfs.Root, error) {
	stat, err := fsx.Stat(p)
	if err != nil {
		return nil, err
	}
	fileSize := stat.Size()
	rs := make([]io.Reader, numWorkers)
	for i := 0; i < numWorkers; i++ {
		start, end := divide(fileSize, numWorkers, i)
		f, err := fsx.OpenFile(p, posixfs.O_RDONLY, 0)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		if n, err := f.Seek(start, io.SeekStart); err != nil {
			return nil, err
		} else if n != start {
			return nil, fmt.Errorf("file seeked to wrong place HAVE: %d WANT: %d", n, start)
		}
		rs[i] = io.LimitReader(f, end-start)
	}
	return fsag.FileFromReaders(ctx, [2]stores.RW{ds, ms}, stat.Mode(), rs)
}

func divide(total int64, numWorkers int, workerIndex int) (start, end int64) {
	start = (total / int64(numWorkers)) * int64(workerIndex)
	end = (total / int64(numWorkers)) * int64(workerIndex+1)
	if end > total {
		end = total
	}
	if workerIndex == numWorkers-1 {
		end = total
	}
	return start, end
}

func createEmptyDir(ctx context.Context, fsag *gotfs.Machine, ms stores.RW) (*gotfs.Root, error) {
	return fsag.NewEmpty(ctx, ms)
}

func changed(a, b *FileInfo) bool {
	return a.ModifiedAt != b.ModifiedAt ||
		a.Mode != b.Mode ||
		a.Size != b.Size
}

func needsUpdate(ctx context.Context, db *DB, p string, finfo posixfs.FileInfo) (bool, error) {
	var ent FileInfo
	ok, err := db.GetInfo(ctx, p, &ent)
	if err != nil {
		return false, err
	}
	if !ok {
		return true, nil
	}
	return ent.ModifiedAt != tai64.FromGoTime(finfo.ModTime()), nil
}
