package gotlob

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/chunking"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
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
	b.lastKey = append(b.lastKey[:0], prefix...)
	return nil
}

// GetPrefix appends the current prefix to out if it exists, or returns nil if it does not
func (b *Builder) GetPrefix(out []byte) []byte {
	for i := len(b.queue) - 1; i >= 0; i-- {
		op := b.queue[i]
		if op.isInline {
			continue
		}
		prefix := b.queue[len(b.queue)-1].key
		return append(out, prefix...)
	}
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
	log.Println("queue len", len(b.queue))
	for i, op := range b.queue {
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
		b.queue[i].lastOffset = offset
		total += length
	}
	li := len(b.queue) - 1
	if b.queue[li].isInline {
		b.queue = b.queue[:0]
	} else {
		b.queue[0] = b.queue[len(b.queue)-1]
		b.queue = b.queue[:1]
	}
	return nil
}

func (b *Builder) copyFrom(ctx context.Context, root Root, span Span) error {
	maxExtentEntry, err := b.op.maxEntry(ctx, b.ms, root, span)
	if err != nil {
		return err
	}
	if maxExtentEntry == nil {
		// just a metadata copy
		it := b.op.gotkv.NewIterator(b.ms, root, span)
		return gotkv.CopyAll(ctx, b.kvb, it)
	}

	span1 := span
	span1.End = maxExtentEntry.Key
	it := b.op.gotkv.NewIterator(b.ms, root, span1)
	// copy one by one until we can fast copy
	for b.chunker.Buffered() > 0 {
		var ent kvstreams.Entry
		if err := it.Next(ctx, &ent); err != nil {
			if err == kvstreams.EOS {
				break
			}
			return err
		}
		if err := b.copyEntry(ctx, ent); err != nil {
			return err
		}
		// blind fast copy
		if err := gotkv.CopyAll(ctx, b.kvb, it); err != nil {
			return err
		}
	}
	// the last extent needs to fill the chunker
	if err := b.copyEntry(ctx, *maxExtentEntry); err != nil {
		return err
	}
	// copy everything after in the span, to get all the metadata keys
	span2 := span
	span2.Begin = gotkv.KeyAfter(maxExtentEntry.Key)
	it2 := b.op.gotkv.NewIterator(b.ms, root, span2)
	if err := gotkv.CopyAll(ctx, b.kvb, it2); err != nil {
		return err
	}
	return nil
}

// copyEntry copies a single entry to the metadata layer.
func (b *Builder) copyEntry(ctx context.Context, ent kvstreams.Entry) error {
	if b.op.keyFilter(ent.Key) {
		ext, err := ParseExtent(ent.Value)
		if err != nil {
			return err
		}
		return b.CopyExtent(ctx, ext)
	} else {
		b.Put(ctx)
	}
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
