package gotfs

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/gotvc/got/pkg/chunking"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

// Builder manages building a filesystem.
type Builder struct {
	o      *Operator
	ctx    context.Context
	ms, ds Store

	dirStack []string

	queue    []pathOp
	chunker  *chunking.ContentDefined
	mBuilder *gotkv.Builder

	root *Root
	err  error
}

func (o *Operator) NewBuilder(ctx context.Context, ms, ds Store) *Builder {
	if ms.MaxSize() < o.maxBlobSize {
		panic(fmt.Sprint("store size too small", ms.MaxSize()))
	}
	if ds.MaxSize() < o.maxBlobSize {
		panic(fmt.Sprint("store size too small", ds.MaxSize()))
	}
	b := &Builder{
		o:   o,
		ctx: ctx,
		ms:  ms,
		ds:  ds,
	}
	b.chunker = o.newChunker(b.handleChunk)
	b.mBuilder = o.gotkv.NewBuilder(b.ms)
	return b
}

// BeginFile creates a metadata entry for a regular file at p and directs Write calls
// to the content of that file.
func (b *Builder) BeginFile(p string, mode os.FileMode) error {
	p = cleanPath(p)
	mode &= ^os.ModeDir
	if b.IsFinished() {
		return errBuilderIsFinished()
	}
	if !mode.IsRegular() {
		return errors.Errorf("mode must be for regular file")
	}
	return b.writeInfo(p, &Info{Mode: uint32(mode)})
}

// Mkdir creates a directory for p.
func (b *Builder) Mkdir(p string, mode os.FileMode) error {
	mode |= os.ModeDir
	if b.IsFinished() {
		return errBuilderIsFinished()
	}
	return b.writeInfo(p, &Info{Mode: uint32(mode)})
}

func (b *Builder) writeInfo(p string, md *Info) error {
	p = cleanPath(p)
	if err := checkPath(p); err != nil {
		return err
	}
	if currentPath := b.currentPath(); currentPath != nil && p <= *currentPath {
		return errors.Errorf("path out of order %q <= %q", p, *currentPath)
	}
	if !parentInStack(p, b.dirStack) {
		return errIncompletePathToRoot(p)
	}
	b.queue = append(b.queue, pathOp{
		path:     p,
		metadata: proto.Clone(md).(*Info),
		isFile:   os.FileMode(md.Mode).IsRegular(),
	})
	if os.FileMode(md.Mode).IsDir() {
		b.dirStack = strings.Split(p, string(Sep))
	}
	return b.flushInfo()
}

// flushInfo attempts to flush the queue to the metadata stream.
// it can't flush any metadata until all the extents before it have been flushed.
func (b *Builder) flushInfo() error {
	var remove int
	for i, op := range b.queue {
		if op.metadata != nil {
			if err := b.putInfo(op.path, op.metadata); err != nil {
				return err
			}
			b.queue[i].metadata = nil
		}
		if op.isFile {
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

func (b *Builder) Write(data []byte) (int, error) {
	if b.IsFinished() {
		return 0, errBuilderIsFinished()
	}
	if err := b.writeData(data); err != nil {
		return 0, err
	}
	return len(data), nil
}

func (b *Builder) writeData(data []byte) error {
	if !b.isInFile() {
		return errors.Errorf("Write called without call to BeginFile")
	}
	l := len(b.queue)
	b.queue[l-1].size += uint64(len(data))
	_, err := b.chunker.Write(data)
	return err
}

// WriteExtents adds extents to the current file.
// Rechunking is avoided, but the last Extent could be short, so it must be rechunked regardless.
// WriteExtents is useful for efficiently joining Extents from disjoint regions of a file.
// See also: Operator.CreateExtents
func (b *Builder) WriteExtents(ctx context.Context, exts []*Extent) error {
	if b.IsFinished() {
		return errBuilderIsFinished()
	}
	for i, ext := range exts {
		isShort := i == len(exts)-1
		if err := b.writeExtent(ext, isShort); err != nil {
			return err
		}
	}
	return nil
}

// writeExtent manages writing an extent to the metadata stream, and copying the data
// to the chunker if necessary.
func (b *Builder) writeExtent(ext *Extent, isShort bool) error {
	if !b.isInFile() {
		return errors.Errorf("WriteExtent called without call to BeginFile")
	}
	if b.chunker.Buffered() == 0 && !isShort {
		for i, op := range b.queue {
			if op.metadata != nil {
				if err := b.putInfo(op.path, op.metadata); err != nil {
					return err
				}
				b.queue[i].metadata = nil
			}
			if op.size-op.written > 0 {
				panic("empty buffer with unwritten data")
			}
		}
		b.queue[0] = b.queue[len(b.queue)-1]
		b.queue = b.queue[:1]

		op := b.queue[0]
		if err := b.putExtent(op.path, op.written, ext); err != nil {
			return err
		}
		b.queue[0].size += uint64(ext.Length)
		b.queue[0].written += uint64(ext.Length)
		return nil
	}
	return b.o.getExtentF(b.ctx, b.ds, ext, func(data []byte) error {
		return b.writeData(data)
	})
}

// handleExtent receives extents, and checks which paths are in them
// then writes metadata to the metadata Builder
func (b *Builder) handleExtent(ext *Extent) error {
	var relOffset uint64
	for i, op := range b.queue {
		// write a metadata entry
		if op.metadata != nil {
			if err := b.putInfo(op.path, op.metadata); err != nil {
				return err
			}
			b.queue[i].metadata = nil
		}
		// if there is unwritten data for this path, write it.
		if op.written < op.size {
			length := uint64(ext.Length) - relOffset
			if (op.size - op.written) < length {
				length = (op.size - op.written)
			}
			ext2 := &Extent{
				Ref:    ext.Ref,
				Offset: uint32(relOffset),
				Length: uint32(length),
			}
			if err := b.putExtent(op.path, op.written, ext2); err != nil {
				return err
			}
			relOffset += length
			b.queue[i].written += length
		}
	}
	b.queue[0] = b.queue[len(b.queue)-1]
	b.queue = b.queue[:1]
	return nil
}

// putExtent write an entry for the extent
func (b *Builder) putExtent(p string, start uint64, ext *Extent) error {
	endOffset := start + uint64(ext.Length)
	k := makeExtentKey(p, endOffset)
	return b.mBuilder.Put(b.ctx, k, ext.marshal())
}

func (b *Builder) putInfo(p string, md *Info) error {
	k := makeInfoKey(p)
	return b.mBuilder.Put(b.ctx, k, md.marshal())
}

func (b *Builder) copyFrom(ctx context.Context, root Root, span gotkv.Span) error {
	maxEnt, err := b.o.gotkv.MaxEntry(ctx, b.ms, root, span)
	if err != nil {
		return err
	}
	if maxEnt == nil {
		return nil
	}
	span.End = maxEnt.Key
	it := b.o.gotkv.NewIterator(b.ms, root, span)
	// copy one by one until we can fast copy
	for b.chunker.Buffered() > 0 || b.haveEnqueuedInfo() {
		var ent kvstreams.Entry
		if err := it.Next(ctx, &ent); err != nil {
			if err == kvstreams.EOS {
				break
			}
			return err
		}
		if err := b.copyEntry(ent); err != nil {
			return err
		}
	}
	// blind fast copy
	if err := gotkv.CopyAll(ctx, b.mBuilder, it); err != nil {
		return err
	}
	// make sure the queue is in the correct state for maxEnt
	if isExtentKey(maxEnt.Key) {
		p, offset, err := splitExtentKey(maxEnt.Key)
		if err != nil {
			return err
		}
		ext, err := parseExtent(maxEnt.Value)
		if err != nil {
			return err
		}
		b.queue = append(b.queue, pathOp{
			path:    p,
			isFile:  true,
			written: offset - uint64(ext.Length),
			size:    offset - uint64(ext.Length),
		})
		if p != "" {
			b.dirStack = strings.Split(parentPath(p), string(Sep))
		}
	} else {
		p, err := parseInfoKey(maxEnt.Key)
		if err != nil {
			return err
		}
		md, err := parseInfo(maxEnt.Value)
		if err != nil {
			return err
		}
		if os.FileMode(md.Mode).IsDir() {
			b.dirStack = strings.Split(p, string(Sep))
		} else if p != "" {
			b.dirStack = strings.Split(parentPath(p), string(Sep))
		}
	}
	// slow copy the last one
	return b.copyEntry(*maxEnt)
}

// copyEntry copies a single entry to the metadata layer.
func (b *Builder) copyEntry(ent kvstreams.Entry) error {
	if isExtentKey(ent.Key) {
		ext, err := parseExtent(ent.Value)
		if err != nil {
			return err
		}
		return b.writeExtent(ext, true)
	} else {
		p, err := parseInfoKey(ent.Key)
		if err != nil {
			return err
		}
		md, err := parseInfo(ent.Value)
		if err != nil {
			return err
		}
		return b.writeInfo(p, md)
	}
}

// handleChunk receives chunks from the chunker and posts them to the store
func (b *Builder) handleChunk(data []byte) error {
	ext, err := b.o.postExtent(b.ctx, b.ds, data)
	if err != nil {
		return err
	}
	return b.handleExtent(ext)
}

// Finish closes the builder and returns the Root to the filesystem.
// Finish is idempotent, and is safe to call multiple times.
// Not calling finish is not an error, the builder does not allocate resources other than memory.
func (b *Builder) Finish() (*Root, error) {
	if !b.IsFinished() {
		b.root, b.err = b.finish()
	}
	return b.root, b.err
}

func (b *Builder) finish() (*Root, error) {
	if err := b.chunker.Flush(); err != nil {
		return nil, err
	}
	for i, op := range b.queue {
		if op.metadata != nil {
			if err := b.putInfo(op.path, op.metadata); err != nil {
				return nil, err
			}
			b.queue[i].metadata = nil
		}
		if op.size-op.written > 0 {
			panic("unwritten extents")
		}
	}
	b.queue = nil
	return b.mBuilder.Finish(b.ctx)
}

func (b *Builder) IsFinished() bool {
	return b.root != nil || b.err != nil
}

func (b *Builder) isInFile() bool {
	l := len(b.queue)
	return l > 0 && b.queue[l-1].isFile
}

func (b *Builder) currentPath() *string {
	if len(b.queue) == 0 {
		return nil
	}
	return &b.queue[len(b.queue)-1].path
}

func (b *Builder) haveEnqueuedInfo() bool {
	for _, op := range b.queue {
		if op.metadata != nil {
			return true
		}
	}
	return false
}

// parentInStack checks is the parent of p
// is in a stack of directories.
// stack == nil means that the root has not been written and all paths
// except for "", which has no parent, will be false
func parentInStack(p string, stack []string) bool {
	if stack == nil && p != "" {
		return false
	}
	parts := strings.Split(p, string(Sep))
	for i := 0; i < len(parts)-1; i++ {
		if i >= len(stack) {
			return false
		}
		if stack[i] != parts[i] {
			return false
		}
	}
	return true
}

func errIncompletePathToRoot(p string) error {
	return errors.Errorf("incomplate path to root for %q", p)
}

func errBuilderIsFinished() error {
	return errors.Errorf("builder is finished")
}

// pathOp is an operation on a path
type pathOp struct {
	path     string
	metadata *Info
	isFile   bool
	size     uint64
	written  uint64
}
