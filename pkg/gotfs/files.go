package gotfs

import (
	"context"
	"io"

	"github.com/gotvc/got/pkg/gotfs/gotlob"
)

// CreateFileRoot creates a new filesystem with the contents read from r at the root
func (o *Operator) CreateFileRoot(ctx context.Context, ms, ds Store, r io.Reader) (*Root, error) {
	b := o.NewBuilder(ctx, ms, ds)
	if err := b.BeginFile("", 0o644); err != nil {
		return nil, err
	}
	_, err := io.Copy(b, r)
	if err != nil {
		return nil, err
	}
	return b.Finish()
}

// CreateExtents returns a list of extents created from r
func (o *Operator) CreateExtents(ctx context.Context, ds Store, r io.Reader) ([]*Extent, error) {
	return o.lob.CreateExtents(ctx, ds, r)
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
	fileRoot, err := o.CreateFileRoot(ctx, ms, ds, r)
	if err != nil {
		return nil, err
	}
	return o.Graft(ctx, ms, ds, x, p, *fileRoot)
}

// SizeOfFile returns the size of the file at p in bytes.
func (o *Operator) SizeOfFile(ctx context.Context, s Store, x Root, p string) (uint64, error) {
	p = cleanPath(p)
	k := makeExtentPrefix(p)
	return o.lob.SizeOf(ctx, s, x, k)
}

// ReadFileAt fills `buf` with data in the file at `p` starting at offset `start`
func (o *Operator) ReadFileAt(ctx context.Context, ms, ds Store, x Root, p string, start int64, buf []byte) (int, error) {
	r, err := o.NewReader(ctx, ms, ds, x, p)
	if err != nil {
		return 0, err
	}
	return r.ReadAt(buf, start)
}

func (o *Operator) NewReader(ctx context.Context, ms, ds Store, x Root, p string) (*gotlob.Reader, error) {
	p = cleanPath(p)
	_, err := o.GetFileInfo(ctx, ms, x, p)
	if err != nil {
		return nil, err
	}
	k := makeExtentPrefix(p)
	return o.lob.NewReader(ctx, ms, ds, x, k)
}
