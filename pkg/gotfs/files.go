package gotfs

import (
	"context"
	"fmt"
	"io"

	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/gotvc/got/pkg/gotfs/gotlob"
	"github.com/gotvc/got/pkg/metrics"
	"golang.org/x/sync/errgroup"
)

func (o *Operator) FileFromReader(ctx context.Context, ms, ds Store, mode posixfs.FileMode, r io.Reader) (*Root, error) {
	return o.FileFromReaders(ctx, ms, ds, mode, []io.Reader{r})
}

// ImportReaders creates a single file at the root from concatenating the data in rs.
// Each reader will be imported from in parallel.
func (o *Operator) FileFromReaders(ctx context.Context, ms, ds Store, mode posixfs.FileMode, rs []io.Reader) (*Root, error) {
	exts := make([][]*Extent, len(rs))
	eg := errgroup.Group{}
	for i, r := range rs {
		i := i
		r := r
		ctx, cf := metrics.Child(ctx, fmt.Sprintf("worker-%d", i))
		eg.Go(func() error {
			defer cf()
			var err error
			exts[i], err = o.lob.CreateExtents(ctx, ds, r)
			return err
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	b := o.NewBuilder(ctx, ms, ds)
	if err := b.BeginFile("", 0o644); err != nil {
		return nil, err
	}
	for i := range exts {
		if err := b.writeExtents(ctx, exts[i]); err != nil {
			return nil, err
		}
	}
	return b.Finish()
}

// CreateFile creates a file at p with data from r
// If there is an entry at p CreateFile returns an error
// ms is the store used for metadata
// ds is the store used for data.
func (o *Operator) CreateFile(ctx context.Context, ms, ds Store, x Root, p string, r io.Reader) (*Root, error) {
	p = cleanPath(p)
	if err := o.checkNoEntry(ctx, ms, x, p); err != nil {
		return nil, err
	}
	fileRoot, err := o.FileFromReader(ctx, ms, ds, 0o755, r)
	if err != nil {
		return nil, err
	}
	return o.Graft(ctx, ms, ds, x, p, *fileRoot)
}

// SizeOfFile returns the size of the file at p in bytes.
func (o *Operator) SizeOfFile(ctx context.Context, s Store, x Root, p string) (uint64, error) {
	p = cleanPath(p)
	k := makeExtentPrefix(p)
	return o.lob.SizeOf(ctx, s, *x.toGotKV(), k)
}

// ReadFileAt fills `buf` with data in the file at `p` starting at offset `start`
func (o *Operator) ReadFileAt(ctx context.Context, ms, ds Store, x Root, p string, start int64, buf []byte) (int, error) {
	r, err := o.NewReader(ctx, ms, ds, x, p)
	if err != nil {
		return 0, err
	}
	return r.ReadAt(buf, start)
}

type Reader = gotlob.Reader

// NewReader returns an io.Reader | io.Seeker | io.ReaderAt
func (o *Operator) NewReader(ctx context.Context, ms, ds Store, x Root, p string) (*Reader, error) {
	p = cleanPath(p)
	_, err := o.GetFileInfo(ctx, ms, x, p)
	if err != nil {
		return nil, err
	}
	k := makeExtentPrefix(p)
	return o.lob.NewReader(ctx, ms, ds, *x.toGotKV(), k)
}
