package gotfs

import (
	"context"
	"fmt"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/chunking"
	"github.com/gotvc/got/pkg/gdat"
)

const (
	maxPartSize = 1 << 20
	avgPartSize = maxPartSize / 2
)

type extentHandler = func(p string, offset uint64, part *Extent) error

type writer struct {
	o        *Operator
	ctx      context.Context
	s        cadata.Store
	onExtent extentHandler

	chunker *chunking.ContentDefined
	path    *string
	offset  uint64
}

func (o *Operator) newWriter(ctx context.Context, s cadata.Store, onExtent extentHandler) *writer {
	if s.MaxSize() < maxPartSize {
		panic(fmt.Sprint("store size too small", s.MaxSize()))
	}
	w := &writer{
		o:        o,
		ctx:      ctx,
		s:        s,
		onExtent: onExtent,
	}
	// TODO: derive hashes from same salt used for convergent encryption.
	w.chunker = chunking.NewContentDefined(avgPartSize, maxPartSize, nil, w.onChunk)
	return w
}

func (w *writer) BeginPath(p string) error {
	if w.path != nil {
		if err := w.chunker.Flush(); err != nil {
			return err
		}
	}
	w.chunker.Reset()
	w.path = &p
	w.offset = 0
	return nil
}

func (w *writer) Write(p []byte) (int, error) {
	if w.path == nil {
		panic("must call BeginPath before Write")
	}
	return w.chunker.Write(p)
}

func (w *writer) Flush() error {
	return w.chunker.Flush()
}

func (w *writer) onChunk(data []byte) error {
	ref, err := w.o.dop.Post(w.ctx, w.s, data)
	if err != nil {
		return err
	}
	ext := &Extent{
		Offset: 0,
		Length: uint32(len(data)),
		Ref:    gdat.MarshalRef(*ref),
	}
	if err := w.onExtent(*w.path, w.offset, ext); err != nil {
		return err
	}
	w.offset += uint64(len(data))
	return nil
}
