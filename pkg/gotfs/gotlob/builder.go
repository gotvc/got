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
	op     *Operator
	ctx    context.Context
	ms, ds cadata.Store

	chunker *chunking.ContentDefined
	kvb     *gotkv.Builder

	lastKey []byte
	queue   []operation
}

func (o *Operator) NewBuilder(ctx context.Context, ms, ds cadata.Store) *Builder {
	b := &Builder{
		op:  o,
		ctx: ctx,
		ms:  ms,
		ds:  ds,
		kvb: o.gotkv.NewBuilder(ms),
	}
	b.chunker = o.newChunker(b.handleChunk)
	return b
}

// Put inserts a literal key, value into key stream.
// Put is not affected by SetPrefix
func (b *Builder) Put(ctx context.Context, key, value []byte) error {
	if len(b.queue) == 0 {
		if err := b.kvb.Put(ctx, key, value); err != nil {
			return err
		}
	} else {
		b.queue = append(b.queue, operation{
			key:      append([]byte{}, key...),
			isInline: true,
			value:    append([]byte{}, value...),
		})
	}
	b.lastKey = append(b.lastKey[:0], key...)
	return nil
}

// SetPrefix set's the prefix used to generate extent keys.
// Extents will be added as <prefix> + < 8 byte BigEndian extent >
func (b *Builder) SetPrefix(prefix []byte) error {
	if err := b.checkKey(prefix); err != nil {
		return err
	}
	b.queue = append(b.queue, operation{
		key:      append([]byte{}, prefix...),
		isInline: false,
	})
	return nil
}

// Write writes extents and creates entries for them under prefix, as specified by the
// last call to SetPrefix
func (b *Builder) Write(data []byte) (int, error) {
	if len(b.queue) == 0 {
		return 0, errors.New("Write called before SetPrefix")
	}
	b.queue[len(b.queue)-1].bytesSent += uint64(len(data))
	return b.chunker.Write(data)
}

// CopyExtent copies an extent to the current object.
func (b *Builder) CopyExtent(ctx context.Context, ext *Extent) error {
	if len(b.queue) == 0 {
		return errors.New("CopyExtent called before Begin")
	}
	if b.chunker.Buffered() > 0 {
		// can't just copy the extent because we are not aligned.
		return b.op.getExtentF(ctx, b.ds, ext, func(data []byte) error {
			_, err := b.Write(data)
			return err
		})
	}
	li := len(b.queue) - 1
	b.queue[li].bytesSent += uint64(ext.Length)
	b.queue[li].lastOffset += b.queue[li].bytesSent
	offset := b.queue[li].lastOffset
	k := make([]byte, 0, 4096)
	k = appendKey(k, b.lastKey, offset)
	return b.kvb.Put(ctx, k, MarshalExtent(ext))
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
	var total uint32
	k := make([]byte, 0, 4096)
	for _, op := range b.queue {
		if op.isInline {
			if err := b.kvb.Put(b.ctx, op.key, op.value); err != nil {
				return err
			}
			continue
		}
		length := uint32(op.bytesSent - op.lastOffset)
		if length > ext.Length-total {
			length = ext.Length - total
		}
		if length == 0 {
			continue
		}
		ext2 := &Extent{
			Ref:    ext.Ref,
			Offset: uint32(total),
			Length: uint32(length),
		}
		offset := op.lastOffset + uint64(length)
		k = appendKey(k[:0], op.key, offset)
		if err := b.kvb.Put(b.ctx, k, MarshalExtent(ext2)); err != nil {
			return err
		}
		op.lastOffset = offset
		total += length
	}
	li := len(b.queue) - 1
	if !b.queue[li].isInline {
		b.queue[0] = b.queue[len(b.queue)-1]
		b.queue = b.queue[:1]
	} else {
		b.queue = b.queue[:0]
	}
	return nil
}

func (b *Builder) checkKey(key []byte) error {
	if cmp := bytes.Compare(key, b.lastKey); cmp <= 0 {
		return fmt.Errorf("%q <= %q", key, b.lastKey)
	}
	return nil
}

type operation struct {
	key []byte

	isInline bool
	// value is an inline value
	value []byte

	// bytesSent is the number of bytes written to the chunker.
	// this is set immediately before writing the bytes.
	// this value should only be modified by the caller
	bytesSent uint64

	// lastOffset is the end offset of the last extent written.
	// this means there was a key set containing lastOffset.
	// this value should only be modifeid by the chunk handler callback.
	lastOffset uint64
}
