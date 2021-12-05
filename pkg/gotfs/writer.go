package gotfs

import (
	"context"
	"fmt"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/chunking"
	"github.com/gotvc/got/pkg/gdat"
)

type extentHandler = func(ext *Extent) error

// writer produces extents
type writer struct {
	o        *Operator
	ctx      context.Context
	s        cadata.Store
	onExtent extentHandler

	chunker *chunking.ContentDefined
}

func (o *Operator) newWriter(ctx context.Context, s cadata.Store, onExtent extentHandler) *writer {
	if s.MaxSize() < o.maxBlobSize {
		panic(fmt.Sprint("store size too small", s.MaxSize()))
	}
	w := &writer{
		o:        o,
		ctx:      ctx,
		s:        s,
		onExtent: onExtent,
	}
	w.chunker = chunking.NewContentDefined(o.minSizeData, o.averageSizeData, o.maxBlobSize, o.poly, w.onChunk)
	return w
}

func (w *writer) Write(p []byte) (int, error) {
	return w.chunker.Write(p)
}

func (w *writer) Flush() error {
	return w.chunker.Flush()
}

func (w *writer) Buffered() int {
	return w.chunker.Buffered()
}

func (w *writer) onChunk(data []byte) error {
	ref, err := w.o.rawOp.Post(w.ctx, w.s, data)
	if err != nil {
		return err
	}
	ext := &Extent{
		Offset: 0,
		Length: uint32(len(data)),
		Ref:    gdat.MarshalRef(*ref),
	}
	return w.onExtent(ext)
}
