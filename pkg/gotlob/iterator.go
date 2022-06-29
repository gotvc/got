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

var _ io.ReadSeeker = &extentReader{}

type extentReader struct {
	ctx      context.Context
	op       *Operator
	ms, ds   cadata.Store
	root     Root
	key      []byte
	streamID uint8
	offset   int64
}

func (r *extentReader) Read(buf []byte) (int, error) {
	n, err := r.ReadAt(buf, r.offset)
	r.offset += int64(n)
	return n, err
}

func (r *extentReader) Seek(off int64, whence int) (int64, error) {
	return r.offset, nil
}

func (r *extentReader) ReadAt(buf []byte, offset int64) (int, error) {
	return 0, nil
}
