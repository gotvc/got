package gotfs

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/gotvc/got/src/gotfs/gotlob"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/state/cadata"
)

type GetPostExister interface {
	cadata.Getter
	cadata.PostExister
}

// Builder manages building a filesystem.
type Builder struct {
	a   *Machine
	ctx context.Context
	ms  stores.RW

	dirStack []string
	b        *gotlob.Builder
}

func (a *Machine) NewBuilder(ctx context.Context, ms, ds stores.RW) *Builder {
	b := &Builder{
		a:   a,
		ctx: ctx,
		ms:  ms,
		b:   a.lob.NewBuilder(ctx, ms, ds),
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
		return fmt.Errorf("mode must be for regular file")
	}
	if err := b.writeInfo(p, &Info{Mode: mode}); err != nil {
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
	return b.writeInfo(p, &Info{Mode: mode})
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

// writeExtents adds extents to the current file.
// Rechunking is avoided, but the last Extent could be short, so it must be rechunked regardless.
// WriteExtents is useful for efficiently joining Extents from disjoint regions of a file.
// See also: Machine.CreateExtents
func (b *Builder) writeExtents(ctx context.Context, exts []*Extent) error {
	if b.IsFinished() {
		return errBuilderIsFinished()
	}
	if err := b.b.CopyExtents(ctx, exts); err != nil {
		return err
	}
	return nil
}

func (b *Builder) copyFrom(ctx context.Context, root gotkv.Root, span gotkv.Span) error {
	if err := b.b.CopyFrom(ctx, root, span); err != nil {
		return err
	}
	p, info, err := b.a.maxInfo(ctx, b.ms, root, span)
	if err != nil {
		return err
	}
	if info != nil {
		if os.FileMode(info.Mode).IsDir() {
			b.dirStack = append(b.dirStack[:0], SplitPath(p)...)
		} else {
			b.dirStack = append(b.dirStack[:0], SplitPath(parentPath(p))...)
		}
	}
	return nil
}

// Finish closes the builder and returns the Root to the filesystem.
// Finish is idempotent, and is safe to call multiple times.
// Not calling finish is not an error, the builder does not allocate resources other than memory.
func (b *Builder) Finish() (*Root, error) {
	root, err := b.b.Finish(b.ctx)
	return newRoot(root), err
}

func (b *Builder) IsFinished() bool {
	return b.b.IsFinished()
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
	return fmt.Errorf("incomplate path to root for %q", p)
}

func errBuilderIsFinished() error {
	return fmt.Errorf("builder is finished")
}

func makeExtentPrefix(p string) []byte {
	return append(makeInfoKey(p), 0x00)
}
