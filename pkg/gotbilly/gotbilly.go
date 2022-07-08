package gotbilly

import (
	"context"
	iofs "io/fs"
	"os"
	"path"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/helper/polyfill"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotiofs"
	"github.com/gotvc/got/pkg/logctx"
)

var _ billy.Basic = &FS{}

type FS struct {
	ctx   context.Context
	b     *branches.Branch
	gotfs gotfs.Operator
}

func New(ctx context.Context, b *branches.Branch) billy.Filesystem {
	fs := &FS{
		ctx:   ctx,
		b:     b,
		gotfs: *branches.NewGotFS(b),
	}
	return polyfill.New(fs)
}

func (fs *FS) Create(filename string) (billy.File, error) {
	return nil, billy.ErrReadOnly
}

func (fs *FS) Join(ps ...string) string {
	return path.Join(ps...)
}

func (fs *FS) Open(p string) (billy.File, error) {
	return fs.OpenFile(p, os.O_RDONLY, 0)
}

func (fs *FS) OpenFile(p string, flag int, perm iofs.FileMode) (billy.File, error) {
	logctx.Infof(fs.ctx, "OpenFile (%s)", p)
	// if flag&(^os.O_RDONLY) > 0 {
	// 	return nil, billy.ErrReadOnly
	// }
	snap, err := branches.GetHead(fs.ctx, *fs.b)
	if err != nil {
		return nil, err
	}
	if snap == nil {
		return nil, iofs.ErrNotExist
	}
	f := newFile(fs.ctx, &fs.gotfs, fs.b.Volume, snap.Root, p)
	return f, nil
}

func (fs *FS) ReadDir(p string) (ret []iofs.FileInfo, retErr error) {
	logctx.Infof(fs.ctx, "ReadDir (%s)", p)
	defer func() { logctx.Infof(fs.ctx, "ReadDir (%s) -> (%v, %v)", p, ret, retErr) }()
	root, err := fs.getRoot()
	if err != nil {
		return nil, err
	}
	if err := fs.gotfs.ReadDir(fs.ctx, fs.b.Volume.FSStore, *root, p, func(de gotfs.DirEnt) error {
		fi, err := gotiofs.Stat(fs.ctx, &fs.gotfs, fs.b.Volume.FSStore, *root, p)
		if err != nil {
			return err
		}
		ret = append(ret, fi)
		return nil
	}); err != nil {
		return nil, err
	}
	return ret, nil
}

func (fs *FS) Remove(p string) error {
	return billy.ErrReadOnly
}

func (fs *FS) Rename(oldpath, newpath string) error {
	return billy.ErrReadOnly
}

func (fs *FS) Stat(p string) (ret iofs.FileInfo, retErr error) {
	logctx.Infof(fs.ctx, "stat (%s)", p)
	defer func() { logctx.Infof(fs.ctx, "stat (%s) -> (%v, %v)", p, ret, retErr) }()
	root, err := fs.getRoot()
	if err != nil {
		return nil, err
	}
	return gotiofs.Stat(fs.ctx, &fs.gotfs, fs.b.Volume.FSStore, *root, p)
}

func (fs *FS) Root() string {
	return ""
}

func (fs *FS) Symlink(target, link string) error {
	return billy.ErrReadOnly
}

func (fs *FS) TempFile(dir, name string) (billy.File, error) {
	return nil, billy.ErrReadOnly
}

func (fs *FS) getRoot() (*gotfs.Root, error) {
	snap, err := branches.GetHead(fs.ctx, *fs.b)
	if err != nil {
		return nil, err
	}
	if snap == nil {
		return nil, iofs.ErrNotExist
	}
	return &snap.Root, nil
}

type File struct {
	ctx   context.Context
	vol   branches.Volume
	gotfs *gotfs.Operator
	root  gotfs.Root
	p     string
	r     *gotfs.Reader
}

func newFile(ctx context.Context, gotfs *gotfs.Operator, vol branches.Volume, root gotfs.Root, p string) *File {
	return &File{
		ctx:   ctx,
		gotfs: gotfs,
		root:  root,
		vol:   vol,
		p:     p,
	}
}

func (f *File) Close() error {
	return nil
}

func (f *File) Lock() error {
	return billy.ErrNotSupported
}

func (f *File) Unlock() error {
	return billy.ErrNotSupported
}

func (f *File) Name() string {
	return path.Base(f.p)
}

func (f *File) Read(buf []byte) (int, error) {
	if err := f.ensureReader(); err != nil {
		return 0, err
	}
	return f.r.Read(buf)
}

func (f *File) ReadAt(buf []byte, offset int64) (int, error) {
	if err := f.ensureReader(); err != nil {
		return 0, err
	}
	return f.r.Read(buf)
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	if err := f.ensureReader(); err != nil {
		return 0, err
	}
	return f.r.Seek(offset, whence)
}

func (f *File) Truncate(x int64) error {
	return billy.ErrNotSupported
}

func (f *File) Write(buf []byte) (int, error) {
	return 0, billy.ErrNotSupported
}

func (f *File)

func (f *File) ensureReader() error {
	if f.r != nil {
		return nil
	}
	r, err := f.gotfs.NewReader(f.ctx, f.vol.FSStore, f.vol.RawStore, f.root, f.p)
	if err != nil {
		return err
	}
	f.r = r
	return nil
}
