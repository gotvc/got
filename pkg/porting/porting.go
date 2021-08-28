// package porting deals with importing and exporting to and from gotfs
package porting

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/fs"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/pkg/errors"
)

func ImportFile(ctx context.Context, fsop *gotfs.Operator, ms, ds cadata.Store, fsx fs.FS, p string) (*gotfs.Root, error) {
	stat, err := fsx.Stat(p)
	if err != nil {
		return nil, err
	}
	if !stat.Mode().IsRegular() {
		return nil, errors.Errorf("ImportFile called for non-regular file at path %q", p)
	}
	f, err := fsx.OpenFile(p, fs.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return fsop.CreateFileRoot(ctx, ms, ds, f.(fs.RegularFile))
}

func ExportFile(ctx context.Context, fsop *gotfs.Operator, ms, ds cadata.Store, root gotfs.Root, fsx fs.FS, p string) error {
	md, err := fsop.GetMetadata(ctx, ms, root, p)
	if err != nil {
		return err
	}
	mode := fs.FileMode(md.Mode)
	if !mode.IsRegular() {
		return errors.Errorf("ExportFile called for non-regular file %q: %v", p, mode)
	}
	r := fsop.NewReader(ctx, ms, ds, root, p)
	return fs.PutFile(ctx, fsx, p, mode, r)
}
