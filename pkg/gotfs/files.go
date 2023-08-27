package gotfs

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/gotvc/got/pkg/gotfs/gotlob"
	"github.com/gotvc/got/pkg/metrics"
	"golang.org/x/sync/errgroup"
)

func (a *Agent) FileFromReader(ctx context.Context, ms, ds Store, mode posixfs.FileMode, r io.Reader) (*Root, error) {
	return a.FileFromReaders(ctx, ms, ds, mode, []io.Reader{r})
}

// ImportReaders creates a single file at the root from concatenating the data in rs.
// Each reader will be imported from in parallel.
func (a *Agent) FileFromReaders(ctx context.Context, ms, ds Store, mode posixfs.FileMode, rs []io.Reader) (*Root, error) {
	exts := make([][]*Extent, len(rs))
	eg := errgroup.Group{}
	for i, r := range rs {
		i := i
		r := r
		ctx, cf := metrics.Child(ctx, fmt.Sprintf("worker-%d", i))
		eg.Go(func() error {
			defer cf()
			var err error
			exts[i], err = a.lob.CreateExtents(ctx, ds, r)
			return err
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	b := a.NewBuilder(ctx, ms, ds)
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
func (a *Agent) CreateFile(ctx context.Context, ms, ds Store, x Root, p string, r io.Reader) (*Root, error) {
	p = cleanPath(p)
	if err := a.checkNoEntry(ctx, ms, x, p); err != nil {
		return nil, err
	}
	return a.PutFile(ctx, ms, ds, x, p, r)
}

// PutFile creates or replaces the file at path using data from r
func (a *Agent) PutFile(ctx context.Context, ms, ds Store, x Root, p string, r io.Reader) (*Root, error) {
	p = cleanPath(p)
	fileRoot, err := a.FileFromReader(ctx, ms, ds, 0o755, r)
	if err != nil {
		return nil, err
	}
	return a.Graft(ctx, ms, ds, x, p, *fileRoot)
}

// SizeOfFile returns the size of the file at p in bytes.
func (a *Agent) SizeOfFile(ctx context.Context, s Store, x Root, p string) (uint64, error) {
	p = cleanPath(p)
	k := makeExtentPrefix(p)
	return a.lob.SizeOf(ctx, s, *x.toGotKV(), k)
}

// ReadFileAt fills `buf` with data in the file at `p` starting at offset `start`
func (a *Agent) ReadFileAt(ctx context.Context, ms, ds Store, x Root, p string, start int64, buf []byte) (int, error) {
	r, err := a.NewReader(ctx, ms, ds, x, p)
	if err != nil {
		return 0, err
	}
	return r.ReadAt(buf, start)
}

type Reader = gotlob.Reader

// NewReader returns an io.Reader | io.Seeker | io.ReaderAt
func (a *Agent) NewReader(ctx context.Context, ms, ds Store, x Root, p string) (*Reader, error) {
	p = cleanPath(p)
	_, err := a.GetFileInfo(ctx, ms, x, p)
	if err != nil {
		return nil, err
	}
	k := makeExtentPrefix(p)
	return a.lob.NewReader(ctx, ms, ds, *x.toGotKV(), k)
}

func (a *Agent) ReadFile(ctx context.Context, ms, ds Store, x Root, p string, max int) ([]byte, error) {
	r, err := a.NewReader(ctx, ms, ds, x, p)
	if err != nil {
		return nil, err
	}
	var n int
	buf := make([]byte, max)
	for n < len(buf) {
		n2, err := r.Read(buf[n:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				n += n2
				return buf[:n], nil
			}
			return nil, err
		}
		n += n2
	}
	return nil, fmt.Errorf("file %q too big. exceeds max of %d", p, max)
}
