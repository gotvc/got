// package porting deals with importing and exporting to and from gotfs
package porting

import (
	"context"
	"fmt"
	"io"
	"path"
	"runtime"

	"errors"

	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/gotvc/got/pkg/metrics"
	"github.com/gotvc/got/pkg/units"
	"go.brendoncarroll.net/state"
	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/state/kv"
	"go.brendoncarroll.net/state/posixfs"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.brendoncarroll.net/tai64"
	"golang.org/x/exp/slices"
)

type Importer struct {
	gotfs *gotfs.Agent
	cache kv.Store[string, Entry]

	ms, ds cadata.Store
}

func NewImporter(fsag *gotfs.Agent, cache kv.Store[string, Entry], ms, ds cadata.Store) *Importer {
	return &Importer{
		gotfs: fsag,
		cache: cache,

		ms: ms,
		ds: ds,
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
	emptyDir, err := createEmptyDir(ctx, pr.gotfs, pr.ms, pr.ds)
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
	slices.SortFunc(dirents, func(a, b posixfs.DirEnt) bool {
		return a.Name < b.Name
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
	return pr.gotfs.Splice(ctx, pr.ms, pr.ds, changes)
}

func (pr *Importer) ImportFile(ctx context.Context, fsx posixfs.FS, p string) (*gotfs.Root, error) {
	return pr.importFile(ctx, fsx, p)
}

// ImportFile returns a gotfs.Root with the content from the file in fsx at p.
func (pr *Importer) importFile(ctx context.Context, fsx posixfs.FS, p string) (*gotfs.Root, error) {
	finfo, err := fsx.Stat(p)
	if err != nil {
		return nil, err
	}
	if !finfo.Mode().IsRegular() {
		return nil, fmt.Errorf("ImportFile called for non-regular file at path %q", p)
	}
	var ent Entry
	if err := pr.cache.Get(ctx, p, &ent); err == nil && ent.ModifiedAt == tai64.FromGoTime(finfo.ModTime()) {
		logctx.Infof(ctx, "using cache entry for path %q. skipped import", p)
		return &ent.Root, nil
	}
	fileSize := finfo.Size()
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
		root, err = pr.gotfs.FileFromReader(ctx, pr.ms, pr.ds, finfo.Mode(), f)
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
	if err := pr.cache.Put(ctx, p, Entry{
		ModifiedAt: tai64.FromGoTime(finfo.ModTime()),
		Root:       *root,
	}); err != nil {
		return nil, err
	}
	return root, nil
}

func importFileConcurrent(ctx context.Context, fsag *gotfs.Agent, ms, ds cadata.Store, fsx posixfs.FS, p string, numWorkers int) (*gotfs.Root, error) {
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
	return fsag.FileFromReaders(ctx, ms, ds, stat.Mode(), rs)
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

func createEmptyDir(ctx context.Context, fsag *gotfs.Agent, ms, ds cadata.Store) (*gotfs.Root, error) {
	return fsag.NewEmpty(ctx, ms)
}

func needsUpdate(ctx context.Context, cache Cache, p string, finfo posixfs.FileInfo) (bool, error) {
	var ent Entry
	err := cache.Get(ctx, p, &ent)
	if errors.Is(err, state.ErrNotFound[string]{Key: p}) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return ent.ModifiedAt != tai64.FromGoTime(finfo.ModTime()), nil
}
