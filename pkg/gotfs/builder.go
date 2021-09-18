package gotfs

import (
	"context"
	"io/fs"
	"runtime"
	sync "sync"

	"github.com/gotvc/got/pkg/gotkv"
	"github.com/gotvc/got/pkg/stores"
)

type builder struct {
	o        *Operator
	ctx      context.Context
	ms, ds   Store
	ads      *stores.AsyncStore
	w        *writer
	mBuilder gotkv.Builder
	path     *string
	offset   uint64

	finishOnce sync.Once
	root       *Root
	err        error
}

func (o *Operator) newBuilder(ctx context.Context, ms, ds Store) *builder {
	b := &builder{
		o:   o,
		ctx: ctx,
		ms:  ms,
		ds:  ds,
		ads: stores.NewAsyncStore(ms, runtime.GOMAXPROCS(0)),
	}
	b.w = o.newWriter(ctx, b.ads, b.handleExtent)
	b.mBuilder = o.gotkv.NewBuilder(b.ms)
	return b
}

func (b *builder) PutMetadata(p string, md *Metadata) error {
	p = cleanPath(p)
	if fs.FileMode(md.Mode).IsRegular() {
		if err := b.w.Flush(); err != nil {
			return err
		}
	}
	if err := checkPath(p); err != nil {
		return err
	}
	b.path = &p
	k := makeMetadataKey(p)
	return b.mBuilder.Put(b.ctx, k, md.marshal())
}

func (b *builder) Write(p []byte) (int, error) {
	return b.w.Write(p)
}

func (b *builder) CopyFrom(ctx context.Context, root Root, span gotkv.Span) error {
	it := b.o.gotkv.NewIterator(b.ms, root, span)
	return gotkv.CopyAll(ctx, b.mBuilder, it)
}

func (b *builder) CopyExtent(ctx context.Context, ext *Extent) error {
	if b.w.Buffered() == 0 {
		p := *b.path
		offset := b.offset
		b.offset += uint64(ext.Length)
		return b.putExtent(p, offset, ext)
	}
	return b.o.getExtentF(ctx, b.ds, ext, func(data []byte) error {
		_, err := b.w.Write(data)
		return err
	})
}

// PutExtent write an entry for the extent
func (b *builder) putExtent(p string, start uint64, ext *Extent) error {
	k := makeExtentKey(p, start+uint64(ext.Length))
	return b.mBuilder.Put(b.ctx, k, ext.marshal())
}

func (b *builder) handleExtent(ext *Extent) error {
	p := *b.path
	offset := b.offset
	b.offset += uint64(ext.Length)
	return b.putExtent(p, offset, ext)
}

func (b *builder) Finish() (*Root, error) {
	b.finishOnce.Do(func() {
		b.root, b.err = b.finish()
	})
	return b.root, b.err
}

func (b *builder) finish() (*Root, error) {
	if err := b.w.Flush(); err != nil {
		return nil, err
	}
	if err := b.ads.Close(); err != nil {
		return nil, err
	}
	return b.mBuilder.Finish(b.ctx)
}
