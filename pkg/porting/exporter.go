package porting

import (
	"context"
	"fmt"

	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/brendoncarroll/go-tai64"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/pkg/errors"
)

type Entry struct {
	ModifiedAt tai64.TAI64N `json:"mtime"`
	Root       gotfs.Root   `json:"root"`
}

type Cache = state.KVStore[string, Entry]

type Exporter struct {
	gotfs *gotfs.Operator
	cache Cache
	fsx   posixfs.FS
}

func NewExporter(gotfs *gotfs.Operator, cache Cache, fsx posixfs.FS) *Exporter {
	return &Exporter{
		gotfs: gotfs,
		cache: cache,
		fsx:   fsx,
	}
}

func (pr *Exporter) ExportFile(ctx context.Context, ms, ds cadata.Store, root gotfs.Root, p string) error {
	md, err := pr.gotfs.GetInfo(ctx, ms, root, p)
	if err != nil {
		return err
	}
	mode := posixfs.FileMode(md.Mode)
	if !mode.IsRegular() {
		return errors.Errorf("ExportFile called for non-regular file %q: %v", p, mode)
	}
	// check if a file exists
	if finfo, err := pr.fsx.Stat(p); err != nil && !posixfs.IsErrNotExist(err) {
		return err
	} else if err == nil {
		if yes, err := needsUpdate(ctx, pr.cache, p, finfo); err != nil {
			return err
		} else if yes {
			return fmt.Errorf("refusing to overwrite unknown file at path %q.  current=%v", p, finfo.ModTime())
		}
	}
	r, err := pr.gotfs.NewReader(ctx, ms, ds, root, p)
	if err != nil {
		return err
	}
	return posixfs.PutFile(ctx, pr.fsx, p, mode, r)
}
