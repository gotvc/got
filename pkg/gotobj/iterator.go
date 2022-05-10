package gotobj

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

	Contents io.ReadSeeker
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
	key, off, err := splitKey(ent.Key)
	if err != nil {
		return err
	}
	return nil
}

type objReader struct {
	op     *Operator
	ms, ds cadata.Store
	span   gotkv.Span

	currentEnt *Entry
	off        uint64
}

func (r *Reader) Read(buf []byte) (int, error) {
	if r.currentEnt == nil {

	}
}

func (r *Reader) Seek(whence int, off int64) {

}
