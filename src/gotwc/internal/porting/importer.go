// package porting deals with importing and exporting to and from gotfs
package porting

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"path"
	"runtime"
	"strings"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/metrics"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/units"
	"go.brendoncarroll.net/state/posixfs"
	"go.brendoncarroll.net/tai64"
	"golang.org/x/exp/slices"
)

type Importer struct {
	gotfs     *gotfs.Machine
	db        *DB
	ss        gotfs.RW
	paramHash [32]byte
}

func NewImporter(fsmach *gotfs.Machine, db *DB, ss gotfs.RW, paramHash [32]byte) *Importer {
	return &Importer{
		gotfs:     fsmach,
		db:        db,
		ss:        ss,
		paramHash: paramHash,
	}
}

// ImportPath returns gotfs instance containing the content in fsx at p.
// The content will be at the root of the filesystem.
func (pr *Importer) ImportPath(ctx context.Context, fsx posixfs.FS, p string) ([]gotfs.Entry, error) {
	finfo, err := fsx.Stat(p)
	if err != nil {
		return nil, err
	}
	if !finfo.Mode().IsDir() {
		return pr.importFile(ctx, fsx, p)
	} else {
		return pr.importDir(ctx, fsx, p, finfo)
	}
}

// importDir lists the contents of the dir and imports all children.
func (pr *Importer) importDir(ctx context.Context, fsx posixfs.FS, p string, finfo fs.FileInfo) ([]gotfs.Entry, error) {
	var changes []gotfs.Segment
	emptyDir, err := createEmptyDir(ctx, pr.gotfs, pr.ss.Metadata, finfo.Mode())
	if err != nil {
		return nil, err
	}
	changes = append(changes, gotfs.Segment{
		Span:     gotkv.TotalSpan(),
		Contents: emptyDir.ToGotKV(),
	})
	dirents, err := posixfs.ReadDir(fsx, p)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(dirents, func(a, b posixfs.DirEnt) int {
		return strings.Compare(a.Name, b.Name)
	})
	metrics.SetDenom(ctx, "paths", len(dirents), "paths")
	var retEnts []gotfs.Entry
	for _, dirent := range dirents {
		ctx, cf := metrics.Child(ctx, dirent.Name)
		defer cf()
		p2 := path.Join(p, dirent.Name)
		ents, err := pr.ImportPath(ctx, fsx, p2)
		if err != nil {
			return nil, err
		}
		metrics.AddInt(ctx, "paths", 1, "paths")
		retEnts = append(retEnts, ents...)
	}
	if p != "" {
		// for directories we don't add the root, just the mode and modified at.
		_, err := pr.db.UpdateInfo(ctx, p, FileInfo{
			Mode:       finfo.Mode(),
			ModifiedAt: tai64.FromGoTime(finfo.ModTime()),
		})
		if err != nil {
			return nil, err
		}
	}
	return retEnts, nil
}

func (pr *Importer) ImportFile(ctx context.Context, fsx posixfs.FS, p string) ([]gotfs.Entry, error) {
	return pr.importFile(ctx, fsx, p)
}

// ImportFile returns a gotfs.Root with the content from the file in fsx at p.
func (pr *Importer) importFile(ctx context.Context, fsx posixfs.FS, p string) ([]gotfs.Entry, error) {
	finfo, err := stat(fsx, p)
	if err != nil {
		return nil, err
	}
	if !finfo.Mode.IsRegular() {
		return nil, fmt.Errorf("ImportFile called for non-regular file at path %q", p)
	}
	if needUpdate, err := pr.db.UpdateInfo(ctx, p, finfo); err != nil {
		return nil, err
	} else if !needUpdate {
		// file has not changed, see if there are extents for this param hash.
		ents, err := pr.db.GetExtents(ctx, p, pr.paramHash, nil)
		if err == nil {
			return ents, nil
		}
	}
	var ent FileInfo
	if ok, err := pr.db.GetInfo(ctx, p, &ent); err != nil {
		return nil, err
	} else if ok && !HasChanged(&ent, &finfo) {
		// TODO: rebuild root from cached extents using gotfs.Builder
		// once gotlob access is available from this package.
	}
	fileSize := finfo.Size
	metrics.SetDenom(ctx, "data_in", int(fileSize), units.Bytes)
	numWorkers := runtime.GOMAXPROCS(0)
	sizeCutoff := 20 * pr.gotfs.MeanBlobSizeData() * numWorkers
	var ents []gotfs.Entry
	if fileSize < int64(sizeCutoff) {
		// fast path for small files
		f, err := fsx.OpenFile(p, posixfs.O_RDONLY, 0)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		exts, err := pr.gotfs.ExtentsFromReader(ctx, pr.ss, f)
		if err != nil {
			return nil, err
		}
		metrics.AddInt(ctx, "data_in", int(fileSize), units.Bytes)
		ents = appendExtents(ents, p, exts)
	} else {
		ents2, err := importFileConcurrent(ctx, pr.gotfs, pr.ss.Metadata, pr.ss.Data, fsx, p, numWorkers)
		if err != nil {
			return nil, err
		}
		ents = append(ents, ents2...)
	}
	if err := pr.db.AddExtents(ctx, p, pr.paramHash, ents); err != nil {
		return nil, err
	}
	return ents, nil
}

func stat(fsys posixfs.FS, p string) (FileInfo, error) {
	finfo, err := fsys.Stat(p)
	if err != nil {
		return FileInfo{}, err
	}
	return FileInfo{
		ModifiedAt: tai64.FromGoTime(finfo.ModTime()),
		Mode:       finfo.Mode(),
		Size:       finfo.Size(),
	}, nil
}

func importFileConcurrent(ctx context.Context, fsag *gotfs.Machine, ms, ds stores.RW, fsx posixfs.FS, p string, numWorkers int) ([]gotfs.Entry, error) {
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
	rawExts, err := fsag.ExtentsFromReaders(ctx, gotfs.RW{ds, ms}, rs)
	if err != nil {
		return nil, err
	}
	return appendExtents(nil, p, rawExts), nil
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

func createEmptyDir(ctx context.Context, fsag *gotfs.Machine, ms stores.RW, mode fs.FileMode) (*gotfs.Root, error) {
	return fsag.NewEmpty(ctx, ms, mode)
}

func HasChanged(a, b *FileInfo) bool {
	return a.ModifiedAt != b.ModifiedAt ||
		a.Mode != b.Mode ||
		(!a.Mode.IsDir() && a.Size != b.Size)
}

func appendExtents(out []gotfs.Entry, p string, exts []gotfs.Extent) []gotfs.Entry {
	var offset uint64
	for _, ext := range exts {
		offset += uint64(ext.Length)
		out = append(out, gotfs.Entry{
			Key:   gotfs.NewExtentKey(p, offset),
			Value: gotfs.Value{Extent: ext},
		})
	}
	return out
}
