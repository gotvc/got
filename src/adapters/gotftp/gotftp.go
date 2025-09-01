package gotftp

import (
	"context"
	"errors"
	"io"
	iofs "io/fs"
	"path"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/adapters/gotiofs"
	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/internal/volumes"
	ftpserver "goftp.io/server/v2"
)

var _ ftpserver.Driver = &Driver{}

type Driver struct {
	ctx   context.Context
	vol   branches.Volume
	gotfs gotfs.Machine
}

func NewDriver(ctx context.Context, info branches.Info, vol branches.Volume) *Driver {
	return &Driver{
		ctx:   ctx,
		vol:   vol,
		gotfs: *branches.NewGotFS(&info),
	}
}

func (d *Driver) Stat(ctx *ftpserver.Context, p string) (iofs.FileInfo, error) {
	root, txn, err := d.getRoot(d.ctx)
	if err != nil {
		return nil, err
	}
	return gotiofs.Stat(d.ctx, &d.gotfs, txn, *root, p)
}

func (d *Driver) GetFile(ctx *ftpserver.Context, p string, off int64) (int64, io.ReadCloser, error) {
	root, txn, err := d.getRoot(d.ctx)
	if err != nil {
		return 0, nil, err
	}
	tx, err := d.vol.BeginTx(d.ctx, blobcache.TxParams{})
	if err != nil {
		return 0, nil, err
	}
	size, err := d.gotfs.SizeOfFile(d.ctx, txn, *root, p)
	if err != nil {
		return 0, nil, err
	}
	f := gotiofs.NewFile(d.ctx, &d.gotfs, tx, *root, p)
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
	root, tx, err := d.getRoot(d.ctx)
	if err != nil {
		return err
	}
	return d.gotfs.ReadDir(d.ctx, tx, *root, p, func(de gotfs.DirEnt) error {
		p2 := path.Join(p, de.Name)
		finfo, err := gotiofs.Stat(d.ctx, &d.gotfs, tx, *root, p2)
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

func (d *Driver) getRoot(ctx context.Context) (*gotfs.Root, volumes.Tx, error) {
	snap, tx, err := branches.GetHead(ctx, d.vol)
	if err != nil {
		return nil, nil, err
	}
	if snap == nil {
		return nil, nil, iofs.ErrNotExist
	}
	return &snap.Root, tx, nil
}

func newErrReadOnly() error {
	return errors.New("filesystem is read-only")
}
