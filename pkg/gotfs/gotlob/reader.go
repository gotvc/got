package gotlob

import (
	"context"
	"fmt"
	"io"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/pkg/errors"
)

var _ io.ReadSeeker = &Reader{}

type Reader struct {
	ctx    context.Context
	o      *Operator
	ms, ds cadata.Store
	root   Root
	prefix []byte

	offset int64
}

func (o *Operator) NewReader(ctx context.Context, ms, ds cadata.Store, root Root, prefix []byte) (*Reader, error) {
	return &Reader{
		ctx:    ctx,
		o:      o,
		ms:     ms,
		ds:     ds,
		root:   root,
		prefix: append([]byte{}, prefix...),
	}, nil
}

func (r *Reader) Read(buf []byte) (int, error) {
	n, err := r.ReadAt(buf, r.offset)
	r.offset += int64(n)
	return n, err
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	size, err := r.o.SizeOf(r.ctx, r.ms, r.root, r.prefix)
	if err != nil {
		return 0, err
	}
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = r.offset + offset
	case io.SeekEnd:
		next = int64(size) - offset
	default:
		return r.offset, errors.Errorf("invalid value for whence %d", whence)
	}
	if next < 0 {
		return r.offset, errors.Errorf("seeked to negative offset: %d", next)
	}
	if next > int64(size) {
		return r.offset, errors.Errorf("seeked past end of file: size=%d, offset=%d", size, next)
	}
	r.offset = next
	return r.offset, nil
}

func (r *Reader) ReadAt(buf []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, fmt.Errorf("invalid offset %d", offset)
	}
	it := r.o.gotkv.NewIterator(r.ms, r.root, gotkv.PrefixSpan(r.prefix))
	var n int
	for n < len(buf) {
		n2, err := r.readFromIterator(r.ctx, it, r.ds, uint64(offset), buf[n:])
		if err != nil {
			if err == gotkv.EOS {
				break
			}
			return n, err
		}
		n += n2
		offset += int64(n2)
	}
	if n > 0 {
		return n, nil
	}
	return n, io.EOF
}

func (r *Reader) readFromIterator(ctx context.Context, it *gotkv.Iterator, ds cadata.Store, start uint64, buf []byte) (int, error) {
	var ent gotkv.Entry
	if err := it.Next(ctx, &ent); err != nil {
		return 0, err
	}
	if !r.o.keyFilter(ent.Key) {
		return 0, nil
	}
	_, extentEnd, err := ParseExtentKey(ent.Key)
	if err != nil {
		return 0, err
	}
	if extentEnd <= start {
		return 0, nil
	}
	ext, err := ParseExtent(ent.Value)
	if err != nil {
		return 0, err
	}
	extentStart := extentEnd - uint64(ext.Length)
	if start < extentStart {
		return 0, errors.Errorf("incorrect extent extentStart=%d asked for start=%d", extentStart, start)
	}
	var n int
	if err := r.o.getExtentF(ctx, ds, ext, func(data []byte) error {
		n += copy(buf, data[start-extentStart:])
		return nil
	}); err != nil {
		return 0, err
	}
	return n, err
}
