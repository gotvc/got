// package gotiofs provides an adapter from Got to an io/fs.FS
package gotiofs

import (
	"context"
	"errors"
	"io"
	iofs "io/fs"
	"path"
	"time"

	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotvc"
	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/state/posixfs"
	"go.brendoncarroll.net/stdctx/logctx"
)

var _ iofs.FS = &FS{}

// FS implements io/fs.FS
type FS struct {
	ctx   context.Context
	gotvc *gotvc.Agent
	gotfs *gotfs.Agent
	vol   *branches.Volume
}

func New(ctx context.Context, info branches.Info, v *branches.Volume) *FS {
	return &FS{
		ctx:   ctx,
		gotvc: branches.NewGotVC(&info),
		gotfs: branches.NewGotFS(&info, gotfs.WithMetaCacheSize(128), gotfs.WithContentCacheSize(16)),
		vol:   v,
	}
}

func (s *FS) Open(name string) (iofs.File, error) {
	logctx.Infof(s.ctx, "open %q", name)
	snap, err := branches.GetHead(s.ctx, *s.vol)
	if err != nil {
		return nil, err
	}
	if snap == nil {
		return nil, iofs.ErrNotExist
	}
	fsag := s.gotfs
	ms := s.vol.FSStore
	if _, err := fsag.GetInfo(s.ctx, ms, snap.Root, name); err != nil {
		return nil, convertError(err)
	}
	return NewFile(s.ctx, fsag, s.vol.FSStore, s.vol.RawStore, snap.Root, name), nil
}

var _ iofs.File = &File{}
var _ iofs.ReadDirFile = &File{}
var _ io.ReaderAt = &File{}
var _ io.Seeker = &File{}

type File struct {
	ctx    context.Context
	gotfs  *gotfs.Agent
	ms, ds cadata.Store
	root   gotfs.Root
	path   string

	r *gotfs.Reader
}

func NewFile(ctx context.Context, fsag *gotfs.Agent, ms, ds cadata.Store, root gotfs.Root, p string) *File {
	return &File{
		ctx:   ctx,
		gotfs: fsag,
		ms:    ms,
		ds:    ds,
		root:  root,
		path:  p,
	}
}

// func (f *File) Write([]byte) (int, error) {
// 	return 0, errors.New("writing not supported")
// }

func (f *File) Read(buf []byte) (int, error) {
	if err := f.ensureReader(); err != nil {
		return 0, err
	}
	n, err := f.r.Read(buf)
	return n, convertError(err)
}

func (f *File) ReadAt(buf []byte, off int64) (int, error) {
	if err := f.ensureReader(); err != nil {
		return 0, err
	}
	if off < 0 {
		return 0, errors.New("negative offset")
	}
	return f.gotfs.ReadFileAt(f.ctx, f.ms, f.ds, f.root, f.path, off, buf)
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	if err := f.ensureReader(); err != nil {
		return 0, err
	}
	n, err := f.r.Seek(offset, whence)
	return n, convertError(err)
}

func (f *File) Stat() (iofs.FileInfo, error) {
	return Stat(f.ctx, f.gotfs, f.ms, f.root, f.path)
}

func (f *File) ReadDir(n int) (ret []iofs.DirEntry, _ error) {
	stopIter := errors.New("stop iteration")
	if err := f.gotfs.ReadDir(f.ctx, f.ms, f.root, f.path, func(e gotfs.DirEnt) error {
		if n > 0 && len(ret) >= n {
			return stopIter
		}
		ret = append(ret, &dirEntry{
			name: e.Name,
			mode: e.Mode,
			getInfo: func() (iofs.FileInfo, error) {
				return f.stat(path.Join(f.path, e.Name))
			},
		})
		return nil
	}); err != nil && !errors.Is(err, stopIter) {
		return nil, err
	}
	return ret, nil
}

func (f *File) Close() error {
	return nil
}

func (f *File) ensureReader() error {
	if f.r == nil {
		r, err := f.gotfs.NewReader(f.ctx, f.ms, f.ds, f.root, f.path)
		if err != nil {
			return err
		}
		f.r = r
	}
	return nil
}

func (f *File) stat(p string) (*fileInfo, error) {
	finfo, err := Stat(f.ctx, f.gotfs, f.ms, f.root, p)
	if err != nil {
		return nil, err
	}
	return finfo.(*fileInfo), nil
}

func Stat(ctx context.Context, fsag *gotfs.Agent, ms cadata.Store, root gotfs.Root, p string) (iofs.FileInfo, error) {
	info, err := fsag.GetInfo(ctx, ms, root, p)
	if err != nil {
		return nil, convertError(err)
	}
	mode := iofs.FileMode(info.Mode)
	var size int64
	if mode.IsRegular() {
		s, err := fsag.SizeOfFile(ctx, ms, root, p)
		if err != nil {
			return nil, convertError(err)
		}
		size = int64(s)
	}
	return &fileInfo{
		name:    path.Base(p),
		mode:    mode,
		size:    size,
		modTime: time.Now(),
	}, nil
}

type fileInfo struct {
	name    string
	mode    iofs.FileMode
	size    int64
	modTime time.Time
}

func (fi fileInfo) Name() string {
	return fi.name
}

func (fi fileInfo) Size() int64 {
	return fi.size
}

func (fi fileInfo) Mode() iofs.FileMode {
	return fi.mode
}

func (fi fileInfo) IsDir() bool {
	return fi.mode.IsDir()
}

func (fi fileInfo) ModTime() time.Time {
	return fi.modTime
}

func (fi fileInfo) Sys() any {
	return nil
}

var _ iofs.DirEntry = &dirEntry{}

type dirEntry struct {
	name    string
	mode    iofs.FileMode
	getInfo func() (iofs.FileInfo, error)
}

func NewDirEntry(x gotfs.DirEnt, getInfo func() (iofs.FileInfo, error)) iofs.DirEntry {
	return &dirEntry{
		name:    x.Name,
		mode:    x.Mode,
		getInfo: getInfo,
	}
}

func (de *dirEntry) Name() string {
	return de.name
}

func (de *dirEntry) IsDir() bool {
	return de.mode.IsDir()
}

func (de *dirEntry) Type() iofs.FileMode {
	return de.mode.Type()
}

func (de *dirEntry) Info() (iofs.FileInfo, error) {
	return de.getInfo()
}

func convertError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, branches.ErrNotExist):
		return iofs.ErrNotExist
	case errors.Is(err, posixfs.ErrNotExist):
		return iofs.ErrNotExist
	default:
		return err
	}
}
