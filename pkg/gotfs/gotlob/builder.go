package gotlob

import (
	"bytes"
	"context"
	"fmt"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/chunking"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
	"github.com/pkg/errors"
)

// Builder chunks large objects, stores them, and then writes extents to a gotkv instance.
type Builder struct {
	op     *Operator
	ctx    context.Context
	ms, ds cadata.Store

	chunker *chunking.ContentDefined
	kvb     *gotkv.Builder

	lastKey []byte
	queue   []operation
	root    *Root
	err     error
}

func (o *Operator) NewBuilder(ctx context.Context, ms, ds cadata.Store) *Builder {
	if ms.MaxSize() < o.gotkv.MaxSize() {
		panic(fmt.Sprint("store size too small", ms.MaxSize()))
	}
	b := &Builder{
		op:  o,
		ctx: ctx,
		ms:  ms,
		ds:  ds,
		kvb: o.gotkv.NewBuilder(ms),
	}
	b.chunker = o.newChunker(b.handleChunk)
	if ds.MaxSize() < b.chunker.MaxSize() {
		panic(fmt.Sprint("store size too small", ds.MaxSize()))
	}
	return b
}

// Put inserts a literal key, value into key stream.
func (b *Builder) Put(ctx context.Context, key, value []byte) error {
	if err := b.checkFinished(); err != nil {
		return err
	}
	if err := b.checkKey(key); err != nil {
		return err
	}
	b.queue = append(b.queue, operation{
		key:      append([]byte{}, key...),
		isInline: true,
		value:    append([]byte{}, value...),
	})
	return b.flushInline(ctx)
}

// SetPrefix set's the prefix used to generate extent keys.
// Extents will be added as <prefix> + < 8 byte BigEndian extent >
func (b *Builder) SetPrefix(prefix []byte) error {
	if err := b.checkFinished(); err != nil {
		return err
	}
	if bytes.Compare(prefix, b.lastKey) < 0 {
		return errors.Errorf("prefix < last key: %q < %q", prefix, b.lastKey)
	}
	if err := b.checkKey(prefix); err != nil {
		return err
	}
	b.queue = append(b.queue, operation{
		key:      append([]byte{}, prefix...),
		isInline: false,
	})
	b.setLastKey(prefix)
	return nil
}

// GetPrefix appends the current prefix to out if it exists, or returns nil if it does not
func (b *Builder) GetPrefix(out []byte) []byte {
	if len(b.queue) == 0 {
		return nil
	}
	op := b.queue[len(b.queue)-1]
	if op.isInline {
		return nil
	}
	prefix := b.queue[len(b.queue)-1].key
	return append(out, prefix...)
}

// Write writes extents and creates entries for them under prefix, as specified by the
// last call to SetPrefix
func (b *Builder) Write(data []byte) (int, error) {
	if err := b.checkFinished(); err != nil {
		return 0, err
	}
	if prefix := b.GetPrefix(nil); prefix == nil {
		return 0, errors.New("Write called before SetPrefix")
	}
	b.queue[len(b.queue)-1].bytesSent += uint64(len(data))
	return b.chunker.Write(data)
}

// CopyExtents copies multiple extents to the current object.
func (b *Builder) CopyExtents(ctx context.Context, exts []*Extent) error {
	if err := b.checkFinished(); err != nil {
		return err
	}
	for i, ext := range exts {
		isShort := i == len(exts)-1
		if err := b.CopyExtent(ctx, ext, isShort); err != nil {
			return err
		}
	}
	return nil
}

func (b *Builder) CopyExtent(ctx context.Context, ext *Extent, isShort bool) error {
	if prefix := b.GetPrefix(nil); prefix == nil {
		return errors.New("CopyExtent called before SetPrefix")
	}
	if b.chunker.Buffered() > 0 || isShort {
		// can't just copy the extent because we are not aligned.
		return b.op.getExtentF(ctx, b.ds, ext, func(data []byte) error {
			_, err := b.Write(data)
			return err
		})
	}
	li := len(b.queue) - 1
	if b.queue[li].bytesSent != b.queue[li].lastOffset {
		panic("data buffered in chunker")
	}
	b.queue[li].bytesSent += uint64(ext.Length)
	b.queue[li].lastOffset = b.queue[li].bytesSent
	offset := b.queue[li].lastOffset
	k := make([]byte, 0, 4096)
	k = appendKey(k, b.queue[li].key, offset)
	return b.kvb.Put(ctx, k, MarshalExtent(ext))
}

func (b *Builder) Finish(ctx context.Context) (*Root, error) {
	if b.root == nil && b.err == nil {
		b.root, b.err = func() (*Root, error) {
			if err := b.chunker.Flush(); err != nil {
				return nil, err
			}
			if err := b.flushInline(ctx); err != nil {
				return nil, err
			}
			return b.kvb.Finish(ctx)
		}()
	}
	return b.root, b.err
}

func (b *Builder) IsFinished() bool {
	return b.root != nil || b.err != nil
}

func (b *Builder) handleChunk(data []byte) error {
	ext, err := b.op.post(b.ctx, b.ds, data)
	if err != nil {
		return err
	}
	var total uint32
	k := make([]byte, 0, gotkv.MaxKeySize)
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
	b.clearQueue()
	return nil
}

// flushInline flushes the inline entries from the queue and clears the queue.
func (b *Builder) flushInline(ctx context.Context) error {
	var remove int
	for _, op := range b.queue {
		if op.isInline {
			if err := b.kvb.Put(ctx, op.key, op.value); err != nil {
				return err
			}
		} else {
			break
		}
		remove++
	}
	if remove > 0 {
		copy(b.queue, b.queue[remove:])
		b.queue = b.queue[:len(b.queue)-remove]
	}
	return nil
}

// clearQueue removes all the elements from the queue, except for the last element if
// it is a non-inline operation.
func (b *Builder) clearQueue() {
	if len(b.queue) == 0 {
		return
	}
	li := len(b.queue) - 1
	if b.queue[li].isInline {
		b.queue = b.queue[:0]
	} else {
		b.queue[0] = b.queue[li]
		b.queue = b.queue[:1]
	}
}

func (b *Builder) CopyFrom(ctx context.Context, root Root, span Span) error {
	if err := b.checkFinished(); err != nil {
		return err
	}
	maxExtKey, maxExt, err := b.op.MaxExtent(ctx, b.ms, root, span)
	if err != nil {
		return err
	}
	span1 := span
	if maxExt != nil {
		span1.End = maxExtKey
		if bytes.Compare(span1.End, span.Begin) < 0 {
			return nil
		}
	}
	it := b.op.gotkv.NewIterator(b.ms, root, span1)
	// copy one by one until we can fast copy
	var ent kvstreams.Entry
	for b.chunker.Buffered() > 0 {
		if err := it.Next(ctx, &ent); err != nil {
			if kvstreams.IsEOS(err) {
				break
			}
			return err
		}
		if b.op.keyFilter(ent.Key) {
			ext, err := ParseExtent(ent.Value)
			if err != nil {
				return err
			}
			if err := b.copyExtentAt(ctx, ent.Key, ext); err != nil {
				return err
			}
		} else {
			if err := b.Put(ctx, ent.Key, ent.Value); err != nil {
				return err
			}
		}
	}
	// blind fast copy
	if err := gotkv.CopyAll(ctx, b.kvb, it); err != nil {
		return err
	}
	// if the max entry was inline, then set the last key, otherwise the maxEnt will take care of it.
	if maxEnt, err := b.op.gotkv.MaxEntry(ctx, b.ms, root, span1); err != nil {
		return err
	} else if maxEnt != nil && !b.op.keyFilter(maxEnt.Key) {
		b.setLastKey(maxEnt.Key)
	}
	// the last extent needs to fill the chunker
	if maxExt != nil {
		prefix, offset, err := ParseExtentKey(maxExtKey)
		if err != nil {
			return err
		}
		b.queue = append(b.queue, operation{
			key:        prefix,
			isInline:   false,
			bytesSent:  offset - uint64(maxExt.Length),
			lastOffset: offset - uint64(maxExt.Length),
		})
		if err := b.copyExtentAt(ctx, maxExtKey, maxExt); err != nil {
			return err
		}
		span2 := span
		span2.Begin = gotkv.KeyAfter(maxExtKey)
		it2 := b.op.gotkv.NewIterator(b.ms, root, span2)
		if err := kvstreams.ForEach(ctx, it2, func(ent kvstreams.Entry) error {
			if b.op.keyFilter(ent.Key) {
				panic(fmt.Sprintf("found extent after max extent. %q", ent.Key))
			}
			return b.Put(ctx, ent.Key, ent.Value)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (b *Builder) copyExtentAt(ctx context.Context, key []byte, ext *Extent) error {
	prefix, _, err := ParseExtentKey(key)
	if err != nil {
		return err
	}
	if oldPrefix := b.GetPrefix(nil); !bytes.Equal(oldPrefix, prefix) {
		if err := b.SetPrefix(prefix); err != nil {
			return err
		}
	}
	if err := b.CopyExtent(ctx, ext, true); err != nil {
		return err
	}
	return nil
}

func (b *Builder) checkKey(key []byte) error {
	if cmp := bytes.Compare(key, b.lastKey); cmp <= 0 {
		return errors.Errorf("%q <= %q", key, b.lastKey)
	}
	return nil
}

func (b *Builder) setLastKey(k []byte) {
	b.lastKey = append(b.lastKey[:0], k...)
}

func (b *Builder) checkFinished() error {
	if b.root != nil || b.err != nil {
		return errors.New("gotlob: Builder has already finished")
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

func (op operation) String() string {
	if op.isInline {
		return fmt.Sprintf("{PUT, %q}", op.value)
	}
	return fmt.Sprintf("{EXT, bytes_sent=%d, last_offset=%d}", op.bytesSent, op.lastOffset)
}
