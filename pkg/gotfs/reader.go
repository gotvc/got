package gotfs

import "context"

type FileReader struct {
	ctx    context.Context
	ms, ds Store
	op     *Operator
	x      Root
	p      string

	offset uint64
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
	n, err := fr.op.ReadFileAt(fr.ctx, fr.ms, fr.ds, fr.x, fr.p, fr.offset, buf)
	fr.offset += uint64(n)
	return n, err
}
