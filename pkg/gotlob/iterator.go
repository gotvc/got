package gotlob

import (
	"context"
	"errors"
	"io"

	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
)

type Object struct {
	Key []byte
}

type Iterator struct {
	ms, ds cadata.Store
	span   state.ByteSpan
	it     gotkv.Iterator
	objKey []byte
}

func (o *Operator) NewIterator(ms, ds cadata.Store, root Root, span state.ByteSpan) *Iterator {
	return &Iterator{
		ms:   ms,
		ds:   ds,
		span: span,
		it:   o.gotkv.NewIterator(ms, root, span),
	}
}

func (it *Iterator) Next(ctx context.Context, o *Object) error {
	var ent gotkv.Entry
	if err := it.it.Peek(ctx, &ent); err != nil && errors.Is(err, kvstreams.EOS) {
		return err
	}
	return nil
}

var _ io.ReadSeeker = &reader{}

type reader struct {
	ctx   context.Context
	ms    cadata.Store
	gotkv *gotkv.Operator
	root  Root
	span  gotkv.Span
	read  func(out []byte, v []byte) (int, error)

	offset int64
}

func (r *reader) Read(buf []byte) (int, error) {
	n, err := r.ReadAt(buf, r.offset)
	r.offset += int64(n)
	return n, err
}

func (r *reader) Seek(off int64, whence int) (int64, error) {
	return r.offset, nil
}

func (r *reader) ReadAt(buf []byte, offset int64) (int, error) {
	return 0, nil
}
