package gotlob

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/chunking"
	"github.com/gotvc/got/pkg/gotkv"
)

type Builder struct {
	op      *Operator
	ctx     context.Context
	ms, ds  cadata.Store
	chunker *chunking.ContentDefined
	kvb     *gotkv.Builder

	//queue      []operation
	currentKey []byte
	streamID   uint8
	offset     uint64
}

func (o *Operator) NewBuilder(ctx context.Context, ms, ds cadata.Store) *Builder {
	b := &Builder{
		op:  o,
		ctx: ctx,
		ms:  ms,
		ds:  ds,
		kvb: o.gotkv.NewBuilder(ms),
	}
	b.chunker = chunking.NewContentDefined(64, DefaultAverageBlobSizeData, DefaultMinBlobSizeData, o.chunkingSeed, b.handleChunk)
	return b
}

func (b *Builder) Begin(key []byte, sid uint8) error {
	if cmp := bytes.Compare(key, b.currentKey); cmp < 0 {
		return fmt.Errorf("%q < %q", key, b.currentKey)
	} else if cmp == 0 && sid <= b.streamID {
		return fmt.Errorf("%d <= %d", sid, b.streamID)
	}
	if err := b.chunker.Flush(); err != nil {
		return err
	}
	b.currentKey = append(b.currentKey[:0], key...)
	b.streamID = sid
	b.offset = 0
	return nil
}

func (b *Builder) CurrentKey(out []byte) []byte {
	return append(out, b.currentKey...)
}

func (b *Builder) Write(data []byte) (int, error) {
	if b.currentKey == nil {
		return 0, errors.New("Write called before Begin")
	}
	if b.streamID >= 128 {
		return b.chunker.Write(data)
	}
	err := strideBytes(data, 4096, func(data []byte) error {
		return b.kvb.Put(b.ctx, b.currentKey, data)
	})
	return len(data), err
}

func (b *Builder) Finish(context.Context) (*Root, error) {
	if err := b.chunker.Flush(); err != nil {
		return nil, err
	}
	return b.kvb.Finish(b.ctx)
}

func (b *Builder) handleChunk(data []byte) error {
	ext, err := b.op.post(b.ctx, b.ds, data)
	if err != nil {
		return err
	}
	b.offset += uint64(len(data))
	k := make([]byte, 0, 4096)
	k = appendKey(k, b.currentKey, b.streamID, b.offset)
	return b.kvb.Put(b.ctx, k, marshalExtent(ext))
}

func strideBytes(data []byte, size int, fn func(data []byte) error) error {
	for i := 0; i < len(data); i += size {
		start := i
		end := i + size
		if end > len(data) {
			end = len(data)
		}
		if err := fn(data[start:end]); err != nil {
			return err
		}
	}
	return nil
}

type operation struct {
	key      []byte
	streamID uint8
	offset   uint64
}
