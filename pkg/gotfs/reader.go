package gotfs

import (
	"context"
	"io"

	"github.com/pkg/errors"
)

var _ io.ReadSeeker = &FileReader{}

type FileReader struct {
	ctx    context.Context
	ms, ds Store
	op     *Operator
	x      Root
	p      string

	offset int64
}

func (o *Operator) NewReader(ctx context.Context, ms, ds Store, x Root, p string) *FileReader {
	return &FileReader{
		ctx: ctx,
		ms:  ms,
		ds:  ds,
		op:  o,

		x: x,
		p: p,
	}
}

func (fr *FileReader) Read(buf []byte) (int, error) {
	n, err := fr.op.ReadFileAt(fr.ctx, fr.ms, fr.ds, fr.x, fr.p, uint64(fr.offset), buf)
	fr.offset += int64(n)
	return n, err
}

func (fr *FileReader) Seek(offset int64, whence int) (int64, error) {
	size, err := fr.op.SizeOfFile(fr.ctx, fr.ms, fr.x, fr.p)
	if err != nil {
		return 0, err
	}
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = fr.offset + offset
	case io.SeekEnd:
		next = int64(size) - offset
	default:
		return fr.offset, errors.Errorf("invalid value for whence %d", whence)
	}
	if next < 0 {
		return fr.offset, errors.Errorf("seeked to negative offset: %d", next)
	}
	if next > int64(size) {
		return fr.offset, errors.Errorf("seeked past end of file: size=%d, offset=%d", size, next)
	}
	fr.offset = next
	return fr.offset, nil
}
