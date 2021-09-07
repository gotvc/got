// package porting deals with importing and exporting to and from gotfs
package porting

import (
	"context"
	"io"
	"path"
	"runtime"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// ImportPath returns gotfs instance containing the content in fsx at p.
// The content will be at the root of the filesystem.
func ImportPath(ctx context.Context, fsop *gotfs.Operator, ms, ds cadata.Store, fsx posixfs.FS, p string) (*gotfs.Root, error) {
	stat, err := fsx.Stat(p)
	if err != nil && !posixfs.IsErrNotExist(err) {
		return nil, err
	} else if posixfs.IsErrNotExist(err) {
		return fsop.NewEmpty(ctx, ms)
	}
	if !stat.Mode().IsDir() {
		return ImportFile(ctx, fsop, ms, ds, fsx, p)
	}
	var changes []gotfs.Segment
	emptyDir, err := createEmptyDir(ctx, fsop, ms, ds)
	if err != nil {
		return nil, err
	}
	changes = append(changes, gotfs.Segment{
		Root: *emptyDir,
		Span: gotkv.TotalSpan(),
	})
	dirents, err := posixfs.ReadDir(fsx, p)
	if err != nil {
		return nil, err
	}
	for _, dirent := range dirents {
		p2 := path.Join(p, dirent.Name)
		pathRoot, err := ImportPath(ctx, fsop, ms, ds, fsx, p2)
		if err != nil {
			return nil, err
		}
		pathRoot, err = fsop.AddPrefix(ctx, ms, dirent.Name, *pathRoot)
		if err != nil {
			return nil, err
		}
		changes = append(changes, gotfs.Segment{
			Root: *pathRoot,
			Span: gotfs.SpanForPath(p),
		})
	}
	return fsop.Splice(ctx, ms, ds, changes)
}

// ImportFile returns a gotfs.Root with the content from the file in fsx at p.
func ImportFile(ctx context.Context, fsop *gotfs.Operator, ms, ds cadata.Store, fsx posixfs.FS, p string) (*gotfs.Root, error) {
	stat, err := fsx.Stat(p)
	if err != nil {
		return nil, err
	}
	if !stat.Mode().IsRegular() {
		return nil, errors.Errorf("ImportFile called for non-regular file at path %q", p)
	}
	fileSize := stat.Size()
	numWorkers := runtime.GOMAXPROCS(0)
	sizeCutoff := 2 * gotfs.DefaultAverageBlobSizeData * numWorkers
	// fast path for small files
	if fileSize < int64(sizeCutoff) {
		f, err := fsx.OpenFile(p, posixfs.O_RDONLY, 0)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		return fsop.CreateFileRoot(ctx, ms, ds, f)
	}
	// for large files use multiple workers
	return importFileConcurrent(ctx, fsop, ms, ds, fsx, p, numWorkers)
}

func importFileConcurrent(ctx context.Context, fsop *gotfs.Operator, ms, ds cadata.Store, fsx posixfs.FS, p string, numWorkers int) (*gotfs.Root, error) {
	stat, err := fsx.Stat(p)
	if err != nil {
		return nil, err
	}
	fileSize := stat.Size()
	eg := errgroup.Group{}
	extSlices := make([][]*gotfs.Extent, numWorkers)
	for i := 0; i < numWorkers; i++ {
		i := i
		start, end := divide(fileSize, numWorkers, i)
		eg.Go(func() error {
			f, err := fsx.OpenFile(p, posixfs.O_RDONLY, 0)
			if err != nil {
				return err
			}
			defer f.Close()
			if _, err := f.Seek(start, io.SeekStart); err != nil {
				return err
			}
			r := io.LimitReader(f, end-start)
			exts, err := fsop.CreateExtents(ctx, ds, r)
			if err != nil {
				return err
			}
			extSlices[i] = exts
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	var extents []*gotfs.Extent
	for i := range extSlices {
		extents = append(extents, extSlices[i]...)
	}
	return fsop.CreateFileRootFromExtents(ctx, ms, ds, extents)
}

func divide(total int64, numWorkers int, workerIndex int) (start, end int64) {
	start = (total / int64(numWorkers)) * int64(workerIndex)
	end = (total / int64(numWorkers)) * int64(workerIndex+1)
	if end > total {
		end = total
	}
	return start, end
}

func ExportFile(ctx context.Context, fsop *gotfs.Operator, ms, ds cadata.Store, root gotfs.Root, fsx posixfs.FS, p string) error {
	md, err := fsop.GetMetadata(ctx, ms, root, p)
	if err != nil {
		return err
	}
	mode := posixfs.FileMode(md.Mode)
	if !mode.IsRegular() {
		return errors.Errorf("ExportFile called for non-regular file %q: %v", p, mode)
	}
	r := fsop.NewReader(ctx, ms, ds, root, p)
	return posixfs.PutFile(ctx, fsx, p, mode, r)
}

func createEmptyDir(ctx context.Context, fsop *gotfs.Operator, ms, ds cadata.Store) (*gotfs.Root, error) {
	empty, err := fsop.NewEmpty(ctx, ms)
	if err != nil {
		return nil, err
	}
	return fsop.Mkdir(ctx, ms, *empty, "")
}
