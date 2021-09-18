// package porting deals with importing and exporting to and from gotfs
package porting

import (
	"context"
	"io"
	"path"
	"runtime"
	"sort"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type Porter struct {
	gotfs   *gotfs.Operator
	posixfs posixfs.FS
	cache   Cache
}

func NewPorter(fsop *gotfs.Operator, pfs posixfs.FS, cache Cache) Porter {
	return Porter{
		cache:   cache,
		gotfs:   fsop,
		posixfs: pfs,
	}
}

// ImportPath returns gotfs instance containing the content in fsx at p.
// The content will be at the root of the filesystem.
func (pr *Porter) ImportPath(ctx context.Context, ms, ds cadata.Store, p string) (*gotfs.Root, error) {
	logrus.Infof("importing path %q", p)
	stat, err := pr.posixfs.Stat(p)
	if err != nil && !posixfs.IsErrNotExist(err) {
		return nil, err
	} else if posixfs.IsErrNotExist(err) {
		return pr.gotfs.NewEmpty(ctx, ms)
	}
	if !stat.Mode().IsDir() {
		return pr.ImportFile(ctx, ms, ds, p)
	}
	var changes []gotfs.Segment
	emptyDir, err := createEmptyDir(ctx, pr.gotfs, ms, ds)
	if err != nil {
		return nil, err
	}
	changes = append(changes, gotfs.Segment{
		Root: *emptyDir,
		Span: gotkv.TotalSpan(),
	})
	dirents, err := posixfs.ReadDir(pr.posixfs, p)
	if err != nil {
		return nil, err
	}
	sort.Slice(dirents, func(i, j int) bool {
		return dirents[i].Name < dirents[j].Name
	})
	for _, dirent := range dirents {
		p2 := path.Join(p, dirent.Name)
		pathRoot, err := pr.ImportPath(ctx, ms, ds, p2)
		if err != nil {
			return nil, err
		}
		pathRoot, err = pr.gotfs.AddPrefix(ctx, ms, dirent.Name, *pathRoot)
		if err != nil {
			return nil, err
		}
		changes = append(changes, gotfs.Segment{
			Root: *pathRoot,
			Span: gotfs.SpanForPath(dirent.Name),
		})
	}
	return pr.gotfs.Splice(ctx, ms, ds, changes)
}

// ImportFile returns a gotfs.Root with the content from the file in fsx at p.
func (pr *Porter) ImportFile(ctx context.Context, ms, ds cadata.Store, p string) (*gotfs.Root, error) {
	stat, err := pr.posixfs.Stat(p)
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
		f, err := pr.posixfs.OpenFile(p, posixfs.O_RDONLY, 0)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		return pr.gotfs.CreateFileRoot(ctx, ms, ds, f)
	}
	// for large files use multiple workers
	return importFileConcurrent(ctx, pr.gotfs, ms, ds, pr.posixfs, p, numWorkers)
}

func importFileConcurrent(ctx context.Context, fsop *gotfs.Operator, ms, ds cadata.Store, fsx posixfs.FS, p string, numWorkers int) (*gotfs.Root, error) {
	stat, err := fsx.Stat(p)
	if err != nil {
		return nil, err
	}
	fileSize := stat.Size()
	logrus.WithFields(logrus.Fields{"path": p, "size": fileSize, "num_workers": numWorkers}).Info("importing file...")
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
			if n, err := f.Seek(start, io.SeekStart); err != nil {
				return err
			} else if n != start {
				return errors.Errorf("file seeked to wrong place HAVE: %d WANT: %d", n, start)
			}
			r := io.LimitReader(f, end-start)
			exts, err := fsop.CreateExtents(ctx, ds, r)
			if err != nil {
				return err
			}
			extSlices[i] = exts
			logrus.WithFields(logrus.Fields{"worker": i, "ext_count": len(exts), "start": start, "end": end}).Info("worker done")
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
	if workerIndex == numWorkers-1 {
		end = total
	}
	return start, end
}

func (pr *Porter) ExportFile(ctx context.Context, ms, ds cadata.Store, root gotfs.Root, p string) error {
	md, err := pr.gotfs.GetMetadata(ctx, ms, root, p)
	if err != nil {
		return err
	}
	mode := posixfs.FileMode(md.Mode)
	if !mode.IsRegular() {
		return errors.Errorf("ExportFile called for non-regular file %q: %v", p, mode)
	}
	r := pr.gotfs.NewReader(ctx, ms, ds, root, p)
	return posixfs.PutFile(ctx, pr.posixfs, p, mode, r)
}

func createEmptyDir(ctx context.Context, fsop *gotfs.Operator, ms, ds cadata.Store) (*gotfs.Root, error) {
	empty, err := fsop.NewEmpty(ctx, ms)
	if err != nil {
		return nil, err
	}
	return fsop.Mkdir(ctx, ms, *empty, "")
}
