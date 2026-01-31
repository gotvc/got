package gotftp

import (
	"context"
	"errors"
	"io"
	iofs "io/fs"
	"path"

	"github.com/gotvc/got/src/adapters/gotiofs"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/internal/marks"
	"github.com/gotvc/got/src/internal/stores"
	ftpserver "goftp.io/server/v2"
)

var _ ftpserver.Driver = &Driver{}

type Driver struct {
	ctx  context.Context
	vctx *marks.ViewCtx
}

func NewDriver(ctx context.Context, vctx *marks.ViewCtx) *Driver {
	return &Driver{
		ctx:  ctx,
		vctx: vctx,
	}
}

func (d *Driver) Stat(ctx *ftpserver.Context, p string) (iofs.FileInfo, error) {
	root, ss, err := d.getRoot(d.ctx)
	if err != nil {
		return nil, err
	}
	return gotiofs.Stat(d.ctx, d.vctx.FS, ss[1], *root, p)
}

func (d *Driver) GetFile(ctx *ftpserver.Context, p string, off int64) (int64, io.ReadCloser, error) {
	root, ss, err := d.getRoot(d.ctx)
	if err != nil {
		return 0, nil, err
	}
	size, err := d.vctx.FS.SizeOfFile(d.ctx, ss[0], *root, p)
	if err != nil {
		return 0, nil, err
	}
	f := gotiofs.NewFile(d.ctx, d.vctx.FS, ss, *root, p)
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
	root, ss, err := d.getRoot(d.ctx)
	if err != nil {
		return err
	}
	fsmach := d.vctx.FS
	return fsmach.ReadDir(d.ctx, ss[1], *root, p, func(de gotfs.DirEnt) error {
		p2 := path.Join(p, de.Name)
		finfo, err := gotiofs.Stat(d.ctx, fsmach, ss[1], *root, p2)
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

func (d *Driver) getRoot(ctx context.Context) (*gotfs.Root, [2]stores.Reading, error) {
	snap := d.vctx.Root
	if snap == nil {
		return nil, [2]stores.Reading{}, iofs.ErrNotExist
	}
	return &snap.Payload.Root, d.vctx.FSRO(), nil
}

func newErrReadOnly() error {
	return errors.New("filesystem is read-only")
}
