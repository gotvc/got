package porting

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv/kvstreams"
	"github.com/gotvc/got/src/internal/stores"

	"go.brendoncarroll.net/state/posixfs"
)

type Exporter struct {
	gotfs  *gotfs.Machine
	db     *Cache
	fsx    posixfs.FS
	filter func(p string) bool
}

func NewExporter(c *Cache, gotfs *gotfs.Machine, fsx posixfs.FS, filter func(p string) bool) *Exporter {
	return &Exporter{
		gotfs:  gotfs,
		db:     c,
		fsx:    fsx,
		filter: filter,
	}
}

// ExportPaths checks what is at p in root, and then exports the directory or file to the filesystem.
func (pr *Exporter) ExportPath(ctx context.Context, ss gotfs.RO, root gotfs.Root, p string) error {
	gfinfo, err := pr.gotfs.GetInfo(ctx, ss.Metadata, root, p)
	if err != nil {
		return err
	}
	ms, ds := ss.Metadata, ss.Data
	if gfinfo.Mode.IsDir() {
		return pr.exportDir(ctx, ms, ds, root, p, gfinfo)
	} else {
		return pr.exportFile(ctx, ms, ds, root, p, gfinfo)
	}
}

func (pr *Exporter) ExportFile(ctx context.Context, ms, ds stores.RO, root gotfs.Root, p string) error {
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

func (pr *Exporter) Clobber(ctx context.Context, ss gotfs.RO, root gotfs.Root, p string) error {
	ms := ss.Metadata
	md, err := pr.gotfs.GetInfo(ctx, ms, root, p)
	if err != nil {
		return err
	}
	if !md.Mode.IsRegular() {
		return fmt.Errorf("clobber can only be called on a single regular file")
	}
	r, err := pr.gotfs.NewReader(ctx, ss, root, p)
	if err != nil {
		return err
	}
	if err := posixfs.PutFile(ctx, pr.fsx, p, md.Mode, r); err != nil {
		return err
	}
	info, err := stat(pr.fsx, p)
	if err != nil {
		return err
	}
	if _, err := pr.db.UpdateInfo(ctx, p, info); err != nil {
		return err
	}
	if err := pr.db.SetOwned(ctx, p, true); err != nil {
		return err
	}
	return nil
}

// exportDir exports a known dir in root
func (pr *Exporter) exportDir(ctx context.Context, ms, ds stores.RO, root gotfs.Root, p string, ginfo *gotfs.Info) error {
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
	if err := pr.gotfs.ReadDir(ctx, ms, root, p, func(e gotfs.DirEnt) error {
		return pr.ExportPath(ctx, gotfs.RO{Metadata: ms, Data: ds}, root, e.Name)
	}); err != nil {
		return err
	}
	return nil
}

// exportFile exports a known file in root
func (pr *Exporter) exportFile(ctx context.Context, ms, ds stores.RO, root gotfs.Root, p string, ginfo *gotfs.Info) error {
	// check if a file exists
	finfo, err := stat(pr.fsx, p)
	if err != nil && !posixfs.IsErrNotExist(err) {
		return err
	} else if err == nil {
		var dbinfo FileInfo
		found, err := pr.db.GetInfo(ctx, p, &dbinfo)
		if err != nil {
			return err
		}
		trackedClean := found && !HasChanged(&dbinfo, &finfo)
		if !trackedClean {
			matches, err := pr.matchesTargetFile(ctx, ms, ds, root, p, ginfo, finfo)
			if err != nil {
				return err
			}
			if !matches {
				return ErrWouldClobber{
					Op:   "write",
					Path: p,
				}
			}
		}
	}
	if err == nil && finfo.Mode.IsDir() {
		if err := pr.deleteDir(ctx, p); err != nil {
			return err
		}
	}
	gfinfo, err := pr.gotfs.GetFileInfo(ctx, ms, root, p)
	if err != nil {
		return err
	}
	r, err := pr.gotfs.NewReader(ctx, gotfs.RO{Metadata: ms, Data: ds}, root, p)
	if err != nil {
		return err
	}
	if err := posixfs.PutFile(ctx, pr.fsx, p, gfinfo.Mode, r); err != nil {
		return err
	}
	nextInfo, err := stat(pr.fsx, p)
	if err != nil {
		return err
	}
	if _, err := pr.db.UpdateInfo(ctx, p, nextInfo); err != nil {
		return err
	}
	if err := pr.db.SetOwned(ctx, p, true); err != nil {
		return err
	}
	return nil
}

func (pr *Exporter) matchesTargetFile(ctx context.Context, ms, ds stores.RO, root gotfs.Root, p string, ginfo *gotfs.Info, finfo FileInfo) (bool, error) {
	if !finfo.Mode.IsRegular() {
		return false, nil
	}
	if finfo.Mode.Perm() != ginfo.Mode.Perm() {
		return false, nil
	}
	targetSize, err := pr.gotfs.SizeOfFile(ctx, ds, root, p)
	if err != nil {
		return false, err
	}
	if uint64(finfo.Size) != targetSize {
		return false, nil
	}
	left, err := pr.fsx.OpenFile(p, os.O_RDONLY, 0)
	if err != nil {
		return false, err
	}
	defer left.Close()
	right, err := pr.gotfs.NewReader(ctx, gotfs.RO{Metadata: ms, Data: ds}, root, p)
	if err != nil {
		return false, err
	}
	bufA := make([]byte, 32*1024)
	bufB := make([]byte, 32*1024)
	for {
		nA, errA := io.ReadFull(left, bufA)
		nB, errB := io.ReadFull(right, bufB)
		if nA != nB || !bytes.Equal(bufA[:nA], bufB[:nB]) {
			return false, nil
		}
		if errA == io.EOF || errA == io.ErrUnexpectedEOF {
			if errB == io.EOF || errB == io.ErrUnexpectedEOF {
				return true, nil
			}
			return false, nil
		}
		if errB == io.EOF || errB == io.ErrUnexpectedEOF {
			return false, nil
		}
		if errA != nil {
			return false, errA
		}
		if errB != nil {
			return false, errB
		}
	}
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
		finfo, err := stat(pr.fsx, p)
		if err != nil {
			return err
		}
		if HasChanged(&finfo, &dbinfo) {
			return ErrWouldClobber{Op: "delete", Path: p}
		}
	}
	if err := pr.fsx.Remove(p); err != nil {
		return err
	}
	if err := pr.db.Delete(ctx, p); err != nil {
		return err
	}
	return pr.db.SetOwned(ctx, p, false)
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
