package gotlob

import (
	"context"
	"io"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/pkg/errors"
)

var _ io.ReadSeeker = &Reader{}

type Reader struct {
	ctx    context.Context
	op     *Operator
	ms     cadata.Store
	root   Root
	prefix []byte
	read   func(out []byte, v []byte) (int, error)

	offset int64
}

func (o *Operator) NewReader(ctx context.Context, ms, ds cadata.Store, root Root, key []byte) (io.ReadSeeker, error) {
	readFn := func(out []byte, v []byte) (int, error) {
		ext, err := ParseExtent(v)
		if err != nil {
			return 0, err
		}
		return o.readExtent(ctx, out, ds, ext)
	}
	return &Reader{
		ctx:    ctx,
		ms:     ms,
		root:   root,
		read:   readFn,
		prefix: append([]byte{}, key...),
	}, nil
}

func (r *Reader) Read(buf []byte) (int, error) {
	n, err := r.ReadAt(buf, r.offset)
	r.offset += int64(n)
	return n, err
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	size, err := r.op.SizeOf(r.ctx, r.ms, r.root, r.prefix)
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
	return 0, nil
}
