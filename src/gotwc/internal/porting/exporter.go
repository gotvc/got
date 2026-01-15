package porting

import (
	"context"
	"fmt"
	"path"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv/kvstreams"
	"github.com/gotvc/got/src/internal/stores"

	"go.brendoncarroll.net/state/posixfs"
)

type Exporter struct {
	gotfs *gotfs.Machine
	db    *DB
	fsx   posixfs.FS
	trash func(string) error
}

func NewExporter(gotfs *gotfs.Machine, db *DB, fsx posixfs.FS, trash func(string) error) *Exporter {
	return &Exporter{
		gotfs: gotfs,
		db:    db,
		fsx:   fsx,
		trash: trash,
	}
}

// ExportSpan exports all of the files and directories in the span.
func (pr *Exporter) ExportSpan(ctx context.Context, ms, ds stores.Reading, root gotfs.Root, span Span) error {
	if !span.IsPrefix() {
		return fmt.Errorf("exporting is only supported to prefix spans")
	}
	dirp := path.Dir(span.Begin)
	return pr.gotfs.ReadDir(ctx, ms, root, dirp, func(de gotfs.DirEnt) error {
		p := path.Join(dirp, de.Name)
		if !span.Contains(p) {
			return nil
		}
		return pr.ExportPath(ctx, ms, ds, root, p)
	})
}

// ExportPaths checks what is at p in root, and then exports the directory or file to the filesystem.
func (pr *Exporter) ExportPath(ctx context.Context, ms, ds stores.Reading, root gotfs.Root, p string) error {
	gfinfo, err := pr.gotfs.GetInfo(ctx, ms, root, p)
	if err != nil {
		return err
	}
	if gfinfo.Mode.IsDir() {
		return pr.exportDir(ctx, ms, ds, root, p)
	} else {
		return pr.exportFile(ctx, ms, ds, root, p)
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
	return pr.exportFile(ctx, ms, ds, root, p)
}

// exportDir exports a known dir in root
func (pr *Exporter) exportDir(ctx context.Context, ms, ds stores.Reading, root gotfs.Root, p string) error {
	return nil
}

// exportFile exports a known file in root
func (pr *Exporter) exportFile(ctx context.Context, ms, ds stores.Reading, root gotfs.Root, p string) error {
	// check if a file exists
	if finfo, err := pr.fsx.Stat(p); err != nil && !posixfs.IsErrNotExist(err) {
		return err
	} else if err == nil {
		if yes, err := needsUpdate(ctx, pr.db, p, finfo); err != nil {
			return err
		} else if yes {
			return fmt.Errorf("refusing to overwrite unknown file at path %q.  current=%v", p, finfo.ModTime())
		}
	}
	gfinfo, err := pr.gotfs.GetFileInfo(ctx, ms, root, p)
	if err != nil {
		return err
	}
	r, err := pr.gotfs.NewReader(ctx, [2]stores.Reading{ms, ds}, root, p)
	if err != nil {
		return err
	}
	if err := pr.trash(p); err != nil {
		return err
	}
	return posixfs.PutFile(ctx, pr.fsx, p, gfinfo.Mode, r)
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
