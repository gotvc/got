package fs

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

type FS interface {
	Stat(p string) (os.FileInfo, error)
	ReadDir(p string, fn func(finfo os.FileInfo) error) error
	Open(p string) (io.ReadCloser, error)
	WriteFile(p string, r io.Reader) error
	Mkdir(p string, mode os.FileMode) error
	Remove(p string) error
}

func IsNotExist(err error) bool {
	return os.IsNotExist(err)
}

func IsExist(err error) bool {
	return os.IsExist(err)
}

type dirFS struct {
	root string
}

func NewDirFS(p string) FS {
	return dirFS{
		root: p,
	}
}

func (fs dirFS) Stat(p string) (os.FileInfo, error) {
	p = filepath.Join(fs.root, p)
	return os.Stat(p)
}

func (fs dirFS) ReadDir(p string, fn func(finfo os.FileInfo) error) error {
	p = filepath.Join(fs.root, p)
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()
	finfos, err := f.Readdir(0)
	if err != nil {
		return err
	}
	for _, finfo := range finfos {
		if err := fn(finfo); err != nil {
			return err
		}
	}
	return nil
}

func (fs dirFS) Open(p string) (io.ReadCloser, error) {
	p = filepath.Join(fs.root, p)
	return os.Open(p)
}

func (fs dirFS) WriteFile(p string, r io.Reader) error {
	p = filepath.Join(fs.root, p)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	f, err := os.Create(p)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := io.Copy(f, r); err != nil {
		return err
	}
	return f.Close()
}

func (fs dirFS) Mkdir(p string, mode os.FileMode) error {
	p = filepath.Join(fs.root, p)
	return os.Mkdir(p, mode)
}

func (fs dirFS) Remove(p string) error {
	p = filepath.Join(fs.root, p)
	return os.Remove(p)
}

type filterFS struct {
	x    FS
	pred func(string) bool
}

func NewFilterFS(x FS, pred func(string) bool) FS {
	return filterFS{x: x, pred: pred}
}

func (fs filterFS) Stat(p string) (os.FileInfo, error) {
	if !fs.pred(p) {
		return nil, os.ErrNotExist
	}
	return fs.x.Stat(p)
}

func (fs filterFS) ReadDir(p string, fn func(os.FileInfo) error) error {
	return fs.x.ReadDir(p, func(finfo os.FileInfo) error {
		p2 := filepath.Join(p, finfo.Name())
		if !fs.pred(p2) {
			return nil
		}
		return fn(finfo)
	})
}

func (fs filterFS) Open(p string) (io.ReadCloser, error) {
	if !fs.pred(p) {
		return nil, os.ErrNotExist
	}
	return fs.x.Open(p)
}

func (fs filterFS) WriteFile(p string, r io.Reader) error {
	if !fs.pred(p) {
		return errors.Errorf("cannot write to path %s", p)
	}
	return fs.x.WriteFile(p, r)
}

func (fs filterFS) Mkdir(p string, mode os.FileMode) error {
	if !fs.pred(p) {
		return errors.Errorf("cannot write to path %s", p)
	}
	return fs.x.Mkdir(p, mode)
}

func (fs filterFS) Remove(p string) error {
	if !fs.pred(p) {
		return errors.Errorf("cannot remove path %s", p)
	}
	return fs.x.Remove(p)
}

func ReadFile(fs FS, p string) ([]byte, error) {
	f, err := fs.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ioutil.ReadAll(f)
}

func WriteFile(fs FS, p string, data []byte) error {
	return fs.WriteFile(p, bytes.NewReader(data))
}

func WriteIfNotExists(fs FS, p string, data []byte) error {
	// TODO: atomically create the file
	_, err := fs.Stat(p)
	if os.IsNotExist(err) {
	} else if err != nil {
		return err
	} else {
		return os.ErrExist
	}
	return WriteFile(fs, p, data)
}

func WalkFiles(ctx context.Context, fs FS, p string, fn func(p string) error) error {
	finfo, err := fs.Stat(p)
	if err != nil {
		return err
	}
	if finfo.IsDir() {
		return fs.ReadDir(p, func(finfo os.FileInfo) error {
			p2 := filepath.Join(p, finfo.Name())
			return WalkFiles(ctx, fs, p2, fn)
		})
	} else {
		return fn(p)
	}
}
