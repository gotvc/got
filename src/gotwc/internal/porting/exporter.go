package porting

import (
	"context"
	"fmt"
	"os"
	"path"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv/kvstreams"
	"github.com/gotvc/got/src/internal/stores"

	"go.brendoncarroll.net/state/posixfs"
	"go.brendoncarroll.net/tai64"
)

type Exporter struct {
	gotfs  *gotfs.Machine
	db     *DB
	fsx    posixfs.FS
	filter func(p string) bool
}

func NewExporter(gotfs *gotfs.Machine, db *DB, fsx posixfs.FS, filter func(p string) bool) *Exporter {
	return &Exporter{
		gotfs:  gotfs,
		db:     db,
		fsx:    fsx,
		filter: filter,
	}
}

// ExportPaths checks what is at p in root, and then exports the directory or file to the filesystem.
func (pr *Exporter) ExportPath(ctx context.Context, ms, ds stores.Reading, root gotfs.Root, p string) error {
	gfinfo, err := pr.gotfs.GetInfo(ctx, ms, root, p)
	if err != nil {
		return err
	}
	if gfinfo.Mode.IsDir() {
		return pr.exportDir(ctx, ms, ds, root, p, gfinfo)
	} else {
		return pr.exportFile(ctx, ms, ds, root, p, gfinfo)
	}
}

func (pr *Exporter) ExportFile(ctx context.Context, ms, ds stores.Reading, root gotfs.Root, p string) error {
	md, err := pr.gotfs.GetInfo(ctx, ms, root, p)
	if err != nil {
		return err
	}
	mode := posixfs.FileMode(md.Mode)
	if !mode.IsRegular() {
		return fmt.Errorf("ExportFile called for non-regular file %q: %v", p, mode)
	}
	return pr.exportFile(ctx, ms, ds, root, p, md)
}

func (pr *Exporter) Clobber(ctx context.Context, ms, ds stores.Reading, root gotfs.Root, p string) error {
	md, err := pr.gotfs.GetInfo(ctx, ms, root, p)
	if err != nil {
		return err
	}
	if !md.Mode.IsRegular() {
		return fmt.Errorf("clobber can only be called on a single regular file")
	}
	r, err := pr.gotfs.NewReader(ctx, [2]stores.Reading{ds, ms}, root, p)
	if err != nil {
		return err
	}
	return posixfs.PutFile(ctx, pr.fsx, p, md.Mode, r)
}

// exportDir exports a known dir in root
func (pr *Exporter) exportDir(ctx context.Context, ms, ds stores.Reading, root gotfs.Root, p string, ginfo *gotfs.Info) error {
	finfo, err := pr.fsx.Stat(p)
	switch {
	case err != nil && !posixfs.IsErrNotExist(err):
		// something went wrong, return
		return err
	case finfo != nil && !finfo.IsDir():
		// file exists, but is not a directory (it should be)
		// If it's not a directory, then remove it.
		if err := pr.deleteFile(ctx, p); err != nil {
			return err
		}
		// and create the directory
		if err := pr.fsx.Mkdir(p, ginfo.Mode.Perm()); err != nil {
			return err
		}
	case finfo != nil:
		// file exists, and is a directory.
		// list the dir entries and delete stuff that doesn't exist in root.
		f, err := pr.fsx.OpenFile(p, os.O_RDONLY, 0)
		if err != nil {
			return err
		}
		defer f.Close()
		ents, err := f.ReadDir(-1)
		if err != nil {
			return err
		}
		for _, ent := range ents {
			p2 := path.Join(p, ent.Name)
			if yes, err := pr.gotfs.Exists(ctx, ms, root, p2); err != nil {
				return err
			} else if !yes {
				if err := pr.deleteFile(ctx, p2); err != nil {
					return err
				}
			}
		}
	default:
		// there's nothing, make a new directory
		if err := pr.fsx.Mkdir(p, ginfo.Mode.Perm()); err != nil {
			return err
		}
	}
	// list all the entries that should exist, and recursively call ExportPath
	return pr.gotfs.ReadDir(ctx, ms, root, p, func(e gotfs.DirEnt) error {
		return pr.ExportPath(ctx, ms, ds, root, e.Name)
	})
}

// exportFile exports a known file in root
func (pr *Exporter) exportFile(ctx context.Context, ms, ds stores.Reading, root gotfs.Root, p string, ginfo *gotfs.Info) error {
	// check if a file exists
	finfo, err := pr.fsx.Stat(p)
	if err != nil && !posixfs.IsErrNotExist(err) {
		return err
	} else if err == nil {
		if yes, err := needsUpdate(ctx, pr.db, p, finfo); err != nil {
			return err
		} else if yes {
			return ErrWouldClobber{
				Op:   "write",
				Path: p,
			}
		}
	}
	if finfo != nil && finfo.IsDir() {
		if err := pr.deleteDir(ctx, p); err != nil {
			return err
		}
	}
	gfinfo, err := pr.gotfs.GetFileInfo(ctx, ms, root, p)
	if err != nil {
		return err
	}
	r, err := pr.gotfs.NewReader(ctx, [2]stores.Reading{ds, ms}, root, p)
	if err != nil {
		return err
	}
	if err := posixfs.PutFile(ctx, pr.fsx, p, gfinfo.Mode, r); err != nil {
		return err
	}
	finfo, err = pr.fsx.Stat(p)
	if err != nil {
		return err
	}
	return pr.db.PutInfo(ctx, FileInfo{
		Path:       p,
		ModifiedAt: tai64.FromGoTime(finfo.ModTime()),
		Mode:       finfo.Mode(),
	})
}

func (pr *Exporter) deleteFile(ctx context.Context, p string) error {
	var dbinfo FileInfo
	if found, err := pr.db.GetInfo(ctx, p, &dbinfo); err != nil {
		return err
	} else if !found {
		return ErrWouldClobber{Op: "delete", Path: p}
	} else if found {
		// We know about this file, we can only delete it if
		// it hasn't been changed since we last checked.
		finfo, err := pr.fsx.Stat(p)
		if err != nil {
			return err
		}
		if tai64.FromGoTime(finfo.ModTime()) != dbinfo.ModifiedAt {
			return ErrWouldClobber{Op: "delete", Path: p}
		}
	}
	if err := pr.fsx.Remove(p); err != nil {
		return err
	}
	return pr.db.Delete(ctx, p)
}

func (pr *Exporter) deleteDir(ctx context.Context, p string) error {
	f, err := pr.fsx.OpenFile(p, os.O_RDONLY, 0)
	if err != nil {
		if posixfs.IsErrNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	ents, err := f.ReadDir(-1)
	if err != nil {
		return err
	}
	for _, ent := range ents {
		p2 := path.Join(p, ent.Name)
		if err := pr.deleteFile(ctx, p2); err != nil {
			return err
		}
	}
	return pr.fsx.Rmdir(p)
}

// Span is a span of paths
type Span struct {
	Begin string
	End   string
}

func (s Span) IsPrefix() bool {
	return s.End == string(kvstreams.PrefixEnd([]byte(s.Begin)))
}

func (s Span) String() string {
	if s.End == "" {
		return fmt.Sprintf("<= %q", s.Begin)
	}
	return fmt.Sprintf("[%q %q)", s.Begin, s.End)
}

func (s Span) Contains(x string) bool {
	if x < s.Begin {
		return false
	}
	if s.End != "" && x >= s.End {
		return false
	}
	return true
}

type ErrWouldClobber struct {
	Op   string
	Path string
}

func (e ErrWouldClobber) Error() string {
	return fmt.Sprintf("export op=%s would clobber path %s", e.Op, e.Path)
}
