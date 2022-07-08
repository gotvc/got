package gotftp

import (
	"context"
	"errors"
	"io"
	iofs "io/fs"
	"path"

	"github.com/gotvc/got/pkg/adapters/gotiofs"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/gotfs"
	ftpserver "goftp.io/server/v2"
)

var _ ftpserver.Driver = &Driver{}

type Driver struct {
	ctx   context.Context
	b     branches.Branch
	gotfs gotfs.Operator
}

func NewDriver(ctx context.Context, b branches.Branch) *Driver {
	return &Driver{
		ctx:   ctx,
		b:     b,
		gotfs: *branches.NewGotFS(&b),
	}
}

func (d *Driver) Stat(ctx *ftpserver.Context, p string) (iofs.FileInfo, error) {
	root, err := d.getRoot()
	if err != nil {
		return nil, err
	}
	return gotiofs.Stat(d.ctx, &d.gotfs, d.b.Volume.FSStore, *root, p)
}

func (d *Driver) GetFile(ctx *ftpserver.Context, p string, off int64) (int64, io.ReadCloser, error) {
	root, err := d.getRoot()
	if err != nil {
		return 0, nil, err
	}
	size, err := d.gotfs.SizeOfFile(d.ctx, d.b.Volume.FSStore, *root, p)
	if err != nil {
		return 0, nil, err
	}
	f := gotiofs.NewFile(d.ctx, d.gotfs, d.b.Volume.FSStore, d.b.Volume.RawStore, *root, p)
	off2, err := f.Seek(off, io.SeekStart)
	if err != nil {
		return 0, nil, err
	}
	return int64(size) - off2, f, nil
}

func (d *Driver) DeleteDir(ctx *ftpserver.Context, p string) error {
	return newErrReadOnly()
}

func (d *Driver) DeleteFile(ctx *ftpserver.Context, p string) error {
	return newErrReadOnly()
}

func (d *Driver) ListDir(ctx *ftpserver.Context, p string, fn func(iofs.FileInfo) error) error {
	root, err := d.getRoot()
	if err != nil {
		return err
	}
	ms := d.b.Volume.FSStore
	return d.gotfs.ReadDir(d.ctx, ms, *root, p, func(de gotfs.DirEnt) error {
		p2 := path.Join(p, de.Name)
		finfo, err := gotiofs.Stat(d.ctx, &d.gotfs, ms, *root, p2)
		if err != nil {
			return err
		}
		return fn(finfo)
	})
}

func (d *Driver) MakeDir(ctx *ftpserver.Context, p string) error {
	return newErrReadOnly()
}

func (d *Driver) PutFile(ctx *ftpserver.Context, p string, r io.Reader, _ int64) (int64, error) {
	return 0, newErrReadOnly()
}

func (d *Driver) Rename(ctx *ftpserver.Context, oldpath, newpath string) error {
	return newErrReadOnly()
}

func (d *Driver) getRoot() (*gotfs.Root, error) {
	snap, err := branches.GetHead(d.ctx, d.b)
	if err != nil {
		return nil, err
	}
	if snap == nil {
		return nil, iofs.ErrNotExist
	}
	return &snap.Root, nil
}

func newErrReadOnly() error {
	return errors.New("filesystem is read-only")
}
