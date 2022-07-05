package gotfs

import (
	"context"
	"os"
	"strings"

	"github.com/gotvc/got/pkg/gotfs/gotlob"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/pkg/errors"
)

// Builder manages building a filesystem.
type Builder struct {
	o   *Operator
	ctx context.Context
	ms  Store

	dirStack []string
	b        *gotlob.Builder

	root *Root
	err  error
}

func (o *Operator) NewBuilder(ctx context.Context, ms, ds Store) *Builder {
	b := &Builder{
		o:   o,
		ctx: ctx,
		ms:  ms,
		b:   o.lob.NewBuilder(ctx, ms, ds),
	}
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
	if err := b.writeInfo(p, &Info{Mode: uint32(mode)}); err != nil {
		return err
	}
	return b.b.SetPrefix(makeExtentPrefix(p))
}

// Mkdir creates a directory for p.
func (b *Builder) Mkdir(p string, mode os.FileMode) error {
	mode |= os.ModeDir
	if b.IsFinished() {
		return errBuilderIsFinished()
	}
	return b.writeInfo(p, &Info{Mode: uint32(mode)})
}

func (b *Builder) writeInfo(p string, info *Info) error {
	p = cleanPath(p)
	if err := checkPath(p); err != nil {
		return err
	}
	if !parentInStack(p, b.dirStack) {
		return errIncompletePathToRoot(p)
	}
	if err := b.b.Put(b.ctx, makeInfoKey(p), info.marshal()); err != nil {
		return err
	}
	if os.FileMode(info.Mode).IsDir() {
		b.dirStack = strings.Split(p, string(Sep))
	}
	return nil
}

func (b *Builder) Write(data []byte) (int, error) {
	if b.IsFinished() {
		return 0, errBuilderIsFinished()
	}
	return b.b.Write(data)
}

// WriteExtents adds extents to the current file.
// Rechunking is avoided, but the last Extent could be short, so it must be rechunked regardless.
// WriteExtents is useful for efficiently joining Extents from disjoint regions of a file.
// See also: Operator.CreateExtents
func (b *Builder) WriteExtents(ctx context.Context, exts []*Extent) error {
	if b.IsFinished() {
		return errBuilderIsFinished()
	}
	if err := b.b.CopyExtents(ctx, exts); err != nil {
		return err
	}
	return nil
}

func (b *Builder) copyFrom(ctx context.Context, root Root, span gotkv.Span) error {
	if err := b.b.CopyFrom(ctx, root, span); err != nil {
		return err
	}
	p, info, err := b.o.MaxInfo(ctx, b.ms, root, span)
	if err != nil {
		return err
	}
	if os.FileMode(info.Mode).IsDir() {
		b.dirStack = append(b.dirStack[:0], SplitPath(p)...)
	} else {
		b.dirStack = append(b.dirStack[:0], SplitPath(parentPath(p))...)
	}
	return nil
}

// Finish closes the builder and returns the Root to the filesystem.
// Finish is idempotent, and is safe to call multiple times.
// Not calling finish is not an error, the builder does not allocate resources other than memory.
func (b *Builder) Finish() (*Root, error) {
	if !b.IsFinished() {
		b.root, b.err = b.finish(b.ctx)
	}
	return b.root, b.err
}

func (b *Builder) finish(ctx context.Context) (*Root, error) {
	return b.b.Finish(ctx)
}

func (b *Builder) IsFinished() bool {
	return b.root != nil || b.err != nil
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

func makeExtentPrefix(p string) []byte {
	return append(makeInfoKey(p), 0x00)
}
