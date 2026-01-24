package gotfs

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/gotvc/got/src/gotfs/gotlob"
	"github.com/gotvc/got/src/internal/metrics"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/state/posixfs"
	"golang.org/x/sync/errgroup"
)

func (mach *Machine) FileFromReader(ctx context.Context, ss [2]stores.RW, mode posixfs.FileMode, r io.Reader) (*Root, error) {
	return mach.FileFromReaders(ctx, ss, mode, []io.Reader{r})
}

// ImportReaders creates a single file at the root from concatenating the data in rs.
// Each reader will be imported from in parallel.
func (mach *Machine) FileFromReaders(ctx context.Context, ss [2]stores.RW, mode posixfs.FileMode, rs []io.Reader) (*Root, error) {
	exts := make([][]*Extent, len(rs))
	eg := errgroup.Group{}
	for i, r := range rs {
		i := i
		r := r
		ctx, cf := metrics.Child(ctx, fmt.Sprintf("worker-%d", i))
		eg.Go(func() error {
			defer cf()
			var err error
			exts[i], err = mach.lob.CreateExtents(ctx, ss[0], r)
			return err
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	b := mach.NewBuilder(ctx, ss[1], ss[0])
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
func (mach *Machine) CreateFile(ctx context.Context, ss [2]stores.RW, x Root, p string, r io.Reader) (*Root, error) {
	p = cleanPath(p)
	if err := mach.checkNoEntry(ctx, ss[1], x, p); err != nil {
		return nil, err
	}
	return mach.PutFile(ctx, ss, x, p, r)
}

// PutFile creates or replaces the file at path using data from r
func (mach *Machine) PutFile(ctx context.Context, ss [2]stores.RW, x Root, p string, r io.Reader) (*Root, error) {
	p = cleanPath(p)
	fileRoot, err := mach.FileFromReader(ctx, ss, 0o755, r)
	if err != nil {
		return nil, err
	}
	return mach.Graft(ctx, ss, x, p, *fileRoot)
}

// SizeOfFile returns the size of the file at p in bytes.
func (mach *Machine) SizeOfFile(ctx context.Context, s stores.Reading, x Root, p string) (uint64, error) {
	p = cleanPath(p)
	k := newInfoKey(p)
	return mach.lob.SizeOf(ctx, s, *x.toGotKV(), k.Prefix(nil))
}

// ReadFileAt fills `buf` with data in the file at `p` starting at offset `start`
func (mach *Machine) ReadFileAt(ctx context.Context, ss [2]stores.Reading, x Root, p string, start int64, buf []byte) (int, error) {
	r, err := mach.NewReader(ctx, ss, x, p)
	if err != nil {
		return 0, err
	}
	return r.ReadAt(buf, start)
}

type Reader = gotlob.Reader

// NewReader returns an io.Reader | io.Seeker | io.ReaderAt
func (mach *Machine) NewReader(ctx context.Context, ss [2]stores.Reading, x Root, p string) (*Reader, error) {
	p = cleanPath(p)
	_, err := mach.GetFileInfo(ctx, ss[1], x, p)
	if err != nil {
		return nil, err
	}
	k := newInfoKey(p)
	return mach.lob.NewReader(ctx, ss[1], ss[0], *x.toGotKV(), k.Prefix(nil))
}

func (mach *Machine) ReadFile(ctx context.Context, ss [2]stores.Reading, x Root, p string, max int) ([]byte, error) {
	r, err := mach.NewReader(ctx, ss, x, p)
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
