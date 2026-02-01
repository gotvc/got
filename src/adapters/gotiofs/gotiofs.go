// package gotiofs provides an adapter from Got to an io/fs.FS
package gotiofs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	iofs "io/fs"
	"path"
	"time"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/internal/gotcore"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/state/posixfs"
	"go.brendoncarroll.net/stdctx/logctx"
)

var _ iofs.FS = &FS{}

// FS implements io/fs.FS
type FS struct {
	ctx  context.Context
	vctx *gotcore.ViewCtx
}

func New(ctx context.Context, vctx *gotcore.ViewCtx) *FS {
	return &FS{
		ctx:  ctx,
		vctx: vctx,
	}
}

func (s *FS) Open(name string) (iofs.File, error) {
	if !fs.ValidPath(name) {
		return nil, fmt.Errorf("bad path %q", name)
	}
	logctx.Infof(s.ctx, "open %q", name)
	var root gotfs.Root
	if s.vctx.Root == nil {
		return nil, iofs.ErrNotExist
	} else {
		root = s.vctx.Root.Payload.Root
	}
	ss := s.vctx.FSRO()
	fsag := s.vctx.FS
	if _, err := fsag.GetInfo(s.ctx, ss[1], root, name); err != nil {
		return nil, convertError(err)
	}
	return NewFile(s.ctx, fsag, ss, root, name), nil
}

var _ iofs.File = &File{}
var _ iofs.ReadDirFile = &File{}
var _ io.ReaderAt = &File{}
var _ io.Seeker = &File{}

type File struct {
	ctx   context.Context
	gotfs *gotfs.Machine
	ss    [2]stores.Reading
	root  gotfs.Root
	path  string

	r          *gotfs.Reader
	dirEntries []iofs.DirEntry
	dirPos     int
	dirLoaded  bool
}

func NewFile(ctx context.Context, fsmach *gotfs.Machine, ss [2]stores.Reading, root gotfs.Root, p string) *File {
	return &File{
		ctx:   ctx,
		gotfs: fsmach,
		ss:    ss,
		root:  root,
		path:  p,
	}
}

func (f *File) Read(buf []byte) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	if err := f.ensureReader(); err != nil {
		return 0, err
	}
	n, err := f.r.Read(buf)
	return n, convertError(err)
}

func (f *File) ReadAt(buf []byte, off int64) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	if err := f.ensureReader(); err != nil {
		return 0, err
	}
	if off < 0 {
		return 0, errors.New("negative offset")
	}
	size, err := f.gotfs.SizeOfFile(f.ctx, f.getStores()[1], f.root, f.path)
	if err != nil {
		return 0, convertError(err)
	}
	if off >= int64(size) {
		return 0, io.EOF
	}
	n, err := f.gotfs.ReadFileAt(f.ctx, f.getStores(), f.root, f.path, off, buf)
	if err != nil {
		return n, convertError(err)
	}
	if n < len(buf) && off+int64(n) >= int64(size) {
		return n, io.EOF
	}
	return n, nil
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	if err := f.ensureReader(); err != nil {
		return 0, err
	}
	if whence == io.SeekEnd {
		size, err := f.gotfs.SizeOfFile(f.ctx, f.getStores()[1], f.root, f.path)
		if err != nil {
			return 0, convertError(err)
		}
		target := int64(size) + offset
		if target < 0 {
			return 0, fmt.Errorf("seeked to negative offset: %d", target)
		}
		n, err := f.r.Seek(target, io.SeekStart)
		return n, convertError(err)
	}
	n, err := f.r.Seek(offset, whence)
	return n, convertError(err)
}

func (f *File) Stat() (iofs.FileInfo, error) {
	return Stat(f.ctx, f.gotfs, f.getStores()[1], f.root, f.path)
}

func (f *File) ReadDir(n int) ([]iofs.DirEntry, error) {
	if err := f.ensureDirEntries(); err != nil {
		return nil, err
	}
	if f.dirPos >= len(f.dirEntries) {
		if n > 0 {
			return nil, io.EOF
		}
		return nil, nil
	}

	remaining := len(f.dirEntries) - f.dirPos
	if n <= 0 || n > remaining {
		n = remaining
	}
	ret := f.dirEntries[f.dirPos : f.dirPos+n]
	f.dirPos += n
	return ret, nil
}

func (f *File) Close() error {
	return nil
}

func (f *File) getStores() [2]stores.Reading {
	return f.ss
}

func (f *File) ensureReader() error {
	if f.r == nil {
		r, err := f.gotfs.NewReader(f.ctx, f.getStores(), f.root, f.path)
		if err != nil {
			return err
		}
		f.r = r
	}
	return nil
}

func (f *File) ensureDirEntries() error {
	if f.dirLoaded {
		return nil
	}
	var entries []iofs.DirEntry
	if err := f.gotfs.ReadDir(f.ctx, f.getStores()[1], f.root, f.path, func(e gotfs.DirEnt) error {
		entries = append(entries, &dirEntry{
			name: e.Name,
			mode: e.Mode,
			getInfo: func() (iofs.FileInfo, error) {
				return f.stat(path.Join(f.path, e.Name))
			},
		})
		return nil
	}); err != nil {
		return err
	}
	f.dirEntries = entries
	f.dirLoaded = true
	return nil
}

func (f *File) stat(p string) (*fileInfo, error) {
	finfo, err := Stat(f.ctx, f.gotfs, f.getStores()[1], f.root, p)
	if err != nil {
		return nil, err
	}
	return finfo.(*fileInfo), nil
}

func Stat(ctx context.Context, fsag *gotfs.Machine, ms stores.Reading, root gotfs.Root, p string) (iofs.FileInfo, error) {
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
		modTime: time.Unix(0, 0),
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
	case errors.Is(err, gotcore.ErrNotExist):
		return iofs.ErrNotExist
	case errors.Is(err, posixfs.ErrNotExist):
		return iofs.ErrNotExist
	default:
		return err
	}
}
