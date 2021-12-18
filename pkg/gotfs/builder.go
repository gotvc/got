package gotfs

import (
	"context"
	"os"
	"strings"
	sync "sync"

	"github.com/gotvc/got/pkg/gotkv"
	"github.com/pkg/errors"
)

// Builder manages building a filesystem.
type Builder struct {
	o      *Operator
	ctx    context.Context
	ms, ds Store

	w        *writer
	mBuilder gotkv.Builder
	dirStack []string
	path     *string
	inFile   bool
	offset   uint64

	finishOnce sync.Once
	root       *Root
	err        error
}

func (o *Operator) NewBuilder(ctx context.Context, ms, ds Store) *Builder {
	b := &Builder{
		o:   o,
		ctx: ctx,
		ms:  ms,
		ds:  ds,
	}
	b.w = o.newWriter(ctx, b.ds, b.handleExtent)
	b.mBuilder = o.gotkv.NewBuilder(b.ms)
	return b
}

// BeginFile creates a metadata entry for a regular file at p and directs Write calls
// to the content of that file.
func (b *Builder) BeginFile(p string, mode os.FileMode) error {
	p = cleanPath(p)
	if b.IsFinished() {
		return errBuilderIsFinished()
	}
	if !mode.IsRegular() {
		return errors.Errorf("mode must be for regular file")
	}
	if b.path != nil && p <= *b.path {
		return errors.Errorf("path out of order %q <= %q", p, *b.path)
	}
	if !parentInStack(p, b.dirStack) {
		return errIncompletePathToRoot(p)
	}
	if err := b.w.Flush(); err != nil {
		return err
	}
	if err := b.putMetadata(p, &Metadata{Mode: uint32(mode)}); err != nil {
		return err
	}
	b.inFile = true
	return nil
}

// Mkdir creates a directory for p.
func (b *Builder) Mkdir(p string, mode os.FileMode) error {
	p = cleanPath(p)
	mode |= os.ModeDir
	if b.IsFinished() {
		return errBuilderIsFinished()
	}
	if b.path != nil && p <= *b.path {
		return errors.Errorf("path out of order %q <= %q", p, *b.path)
	}
	if b.dirStack == nil && p != "" {
		return errIncompletePathToRoot(p)
	}
	if !parentInStack(p, b.dirStack) {
		return errIncompletePathToRoot(p)
	}
	if err := b.putMetadata(p, &Metadata{Mode: uint32(mode)}); err != nil {
		return err
	}
	b.dirStack = strings.Split(p, string(Sep))
	b.inFile = false
	return nil
}

func (b *Builder) putMetadata(p string, md *Metadata) error {
	if err := checkPath(p); err != nil {
		return err
	}
	b.path = &p
	k := makeMetadataKey(p)
	return b.mBuilder.Put(b.ctx, k, md.marshal())
}

func (b *Builder) Write(data []byte) (int, error) {
	if b.IsFinished() {
		return 0, errBuilderIsFinished()
	}
	if !b.inFile {
		return 0, errors.Errorf("Write called without call to BeginFile")
	}
	return b.w.Write(data)
}

func (b *Builder) WriteExtent(ctx context.Context, ext *Extent) error {
	if b.IsFinished() {
		return errBuilderIsFinished()
	}
	if !b.inFile {
		return errors.Errorf("WriteExtent called without call to BeginFile")
	}
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
func (b *Builder) putExtent(p string, start uint64, ext *Extent) error {
	k := makeExtentKey(p, start+uint64(ext.Length))
	return b.mBuilder.Put(b.ctx, k, ext.marshal())
}

func (b *Builder) copyFrom(ctx context.Context, root Root, span gotkv.Span) error {
	it := b.o.gotkv.NewIterator(b.ms, root, span)
	return gotkv.CopyAll(ctx, b.mBuilder, it)
}

func (b *Builder) handleExtent(ext *Extent) error {
	p := *b.path
	offset := b.offset
	b.offset += uint64(ext.Length)
	return b.putExtent(p, offset, ext)
}

// Finish closes
func (b *Builder) Finish() (*Root, error) {
	b.finishOnce.Do(func() {
		b.root, b.err = b.finish()
	})
	return b.root, b.err
}

func (b *Builder) finish() (*Root, error) {
	if err := b.w.Flush(); err != nil {
		return nil, err
	}
	return b.mBuilder.Finish(b.ctx)
}

func (b *Builder) IsFinished() bool {
	return b.root != nil || b.err != nil
}

func parentInStack(p string, stack []string) bool {
	parts := strings.Split(p, string(Sep))
	if len(parts)-1 > len(stack) {
		return false
	}
	for i := 0; i < len(parts)-1 && i < len(stack); i++ {
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
