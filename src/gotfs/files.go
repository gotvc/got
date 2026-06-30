package gotfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"

	"github.com/gotvc/got/src/gotfs/gotlob"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/metrics"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/streams"
	"golang.org/x/sync/errgroup"
)

func (mach *Machine) ExtentsFromReader(ctx context.Context, ss RW, r io.Reader) ([]Extent, error) {
	return mach.ExtentsFromReaders(ctx, ss, []io.Reader{r})
}

func (mach *Machine) ExtentsFromReaders(ctx context.Context, ss RW, rs []io.Reader) ([]Extent, error) {
	exts := make([][]Extent, len(rs))
	eg := errgroup.Group{}
	for i, r := range rs {
		i := i
		r := r
		ctx, cf := metrics.Child(ctx, fmt.Sprintf("worker-%d", i))
		eg.Go(func() error {
			defer cf()
			var err error
			exts[i], err = mach.lob.CreateExtents(ctx, ss.Data, r)
			return err
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	b := mach.NewBuilder(ctx, ss)
	if err := b.BeginFile("", 0o644); err != nil {
		return nil, err
	}
	for i := range exts {
		if err := b.writeExtents(ctx, exts[i]); err != nil {
			return nil, err
		}
	}
	root, err := b.Finish()
	if err != nil {
		return nil, err
	}
	var retExts []Extent
	it := mach.NewIterator(ss.Metadata, *root, gotkv.TotalSpan())
	if err := streams.ForEach(ctx, &it, func(ent Entry) error {
		if !ent.Key.IsInfo() {
			retExts = append(retExts, ent.Value.Extent)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return retExts, nil
}

func (mach *Machine) newFile(ctx context.Context, ss RW, mode fs.FileMode, exts []Extent) (*Root, error) {
	b := mach.NewBuilder(ctx, ss)
	if err := b.BeginFile("", mode); err != nil {
		return nil, err
	}
	if err := b.writeExtents(ctx, exts); err != nil {
		return nil, err
	}
	return b.Finish()
}

// CreateFile creates a file at p with data from r
// If there is an entry at p CreateFile returns an error
func (mach *Machine) CreateFile(ctx context.Context, ss RW, x Root, p string, r io.Reader) (*Root, error) {
	p = cleanPath(p)
	if err := mach.checkNoEntry(ctx, ss.Metadata, x, p); err != nil {
		return nil, err
	}
	return mach.PutFile(ctx, ss, x, p, r)
}

// PutFile creates or replaces the file at path using data from r
func (mach *Machine) PutFile(ctx context.Context, ss RW, x Root, p string, r io.Reader) (*Root, error) {
	p = cleanPath(p)
	exts, err := mach.ExtentsFromReader(ctx, ss, r)
	if err != nil {
		return nil, err
	}
	fileRoot, err := mach.newFile(ctx, ss, 0o644, exts)
	if err != nil {
		return nil, err
	}
	return mach.Graft(ctx, ss, x, p, *fileRoot)
}

// SizeOfFile returns the size of the file at p in bytes.
func (mach *Machine) SizeOfFile(ctx context.Context, s stores.RO, x Root, p string) (uint64, error) {
	p = cleanPath(p)
	k := newInfoKey(p)
	return mach.lob.SizeOf(ctx, s, x.toGotKV(), k.Prefix(nil))
}

// ReadFileAt fills `buf` with data in the file at `p` starting at offset `start`
func (mach *Machine) ReadFileAt(ctx context.Context, ss RO, x Root, p string, start int64, buf []byte) (int, error) {
	r, err := mach.NewReader(ctx, ss, x, p)
	if err != nil {
		return 0, err
	}
	return r.ReadAt(buf, start)
}

type Reader = gotlob.Reader

// NewReader returns an io.Reader | io.Seeker | io.ReaderAt
func (mach *Machine) NewReader(ctx context.Context, ss RO, x Root, p string) (*Reader, error) {
	p = cleanPath(p)
	_, err := mach.GetFileInfo(ctx, ss.Metadata, x, p)
	if err != nil {
		return nil, err
	}
	k := newInfoKey(p)
	return mach.lob.NewReader(ctx, ss.Metadata, ss.Data, x.toGotKV(), k.Prefix(nil))
}

func (mach *Machine) ReadFile(ctx context.Context, ss RO, x Root, p string, max int) ([]byte, error) {
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
