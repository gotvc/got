package gotfs

import (
	"context"
	"io"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/pkg/errors"
)

// CreateFileRoot creates a new filesystem with the contents read from r at the root
func (o *Operator) CreateFileRoot(ctx context.Context, ms, ds Store, r io.Reader) (*Root, error) {
	b := o.newBuilder(ctx, ms, ds)
	defer b.Finish()
	// metadata entry
	md := Metadata{
		Mode: 0o644,
	}
	if err := b.PutMetadata("", &md); err != nil {
		return nil, err
	}
	if _, err := io.Copy(b, r); err != nil {
		return nil, err
	}
	return b.Finish()
}

// CreateExtents returns a list of extents created from r
func (o *Operator) CreateExtents(ctx context.Context, ds Store, r io.Reader) ([]*Extent, error) {
	var exts []*Extent
	w := o.newWriter(ctx, ds, func(ext *Extent) error {
		exts = append(exts, ext)
		return nil
	})
	if _, err := io.Copy(w, r); err != nil {
		return nil, err
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	return exts, nil
}

func (o *Operator) CreateFileRootFromExtents(ctx context.Context, ms, ds Store, exts []*Extent) (*Root, error) {
	b := o.newBuilder(ctx, ms, ds)
	if err := b.PutMetadata("", &Metadata{
		Mode: 0o644,
	}); err != nil {
		return nil, err
	}
	for _, ext := range exts {
		if err := b.CopyExtent(ctx, ext); err != nil {
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
	fileRoot, err := o.CreateFileRoot(ctx, ms, ds, r)
	if err != nil {
		return nil, err
	}
	return o.Graft(ctx, ms, x, p, *fileRoot)
}

// SizeOfFile returns the size of the file at p in bytes.
func (o *Operator) SizeOfFile(ctx context.Context, s Store, x Root, p string) (uint64, error) {
	p = cleanPath(p)
	k := makeMetadataKey(p)
	under := append(k, 0x01)
	key, err := o.gotkv.MaxKey(ctx, s, x, under)
	if err != nil {
		return 0, err
	}
	_, offset, err := splitExtentKey(key)
	return offset, err
}

// ReadFileAt fills `buf` with data in the file at `p` starting at offset `start`
func (o *Operator) ReadFileAt(ctx context.Context, ms, ds Store, x Root, p string, start uint64, buf []byte) (int, error) {
	p = cleanPath(p)
	_, err := o.GetFileMetadata(ctx, ms, x, p)
	if err != nil {
		return 0, err
	}
	key := makeExtentKey(p, start)
	span := gotkv.Span{
		Start: key,
		End:   fileSpanEnd(p),
	}
	it := o.gotkv.NewIterator(ms, x, span)
	var n int
	for n < len(buf) {
		n2, err := o.readFromIterator(ctx, it, ds, start, buf[n:])
		if err != nil {
			if err == io.EOF {
				break
			}
			return n, err
		}
		n += n2
		start += uint64(n2)
	}
	if n > 0 {
		return n, nil
	}
	return n, io.EOF
}

func (o *Operator) readFromIterator(ctx context.Context, it gotkv.Iterator, ds cadata.Store, start uint64, buf []byte) (int, error) {
	ent, err := it.Next(ctx)
	if err != nil {
		return 0, err
	}
	_, extentEnd, err := splitExtentKey(ent.Key)
	if err != nil {
		return 0, err
	}
	if extentEnd <= start {
		return 0, nil
	}
	ext, err := parseExtent(ent.Value)
	if err != nil {
		return 0, err
	}
	extentStart := extentEnd - uint64(ext.Length)
	if start < extentStart {
		return 0, errors.Errorf("incorrect extent extentStart=%d asked for start=%d", extentEnd, start)
	}
	var n int
	if err := o.getExtentF(ctx, ds, ext, func(data []byte) error {
		n += copy(buf, data[start-extentStart:])
		return nil
	}); err != nil {
		return 0, err
	}
	return n, err
}

func (o *Operator) WriteFileAt(ctx context.Context, s Store, x Root, p string, start uint64, data []byte) (*Ref, error) {
	md, err := o.GetFileMetadata(ctx, s, x, p)
	if err != nil {
		return nil, err
	}
	panic(md)
}
