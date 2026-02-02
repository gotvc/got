package gotwc

import (
	"context"
	"strings"

	"github.com/gotvc/got/src/gotwc/internal/porting"
	"github.com/gotvc/got/src/gotwc/internal/sqlutil"
	"go.brendoncarroll.net/exp/maybe"
	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/state/posixfs"
)

// unknownFile is a path in the working directory which has changed.
type unknownFile struct {
	// Known is the last known version of the file recorded in the database.
	Known maybe.Maybe[FileInfo]
	// Current is the most recent observation of the file in the filesystem.
	Current maybe.Maybe[FileInfo]
}

func (ukp *unknownFile) Path() string {
	switch {
	case ukp.Known.Ok:
		return ukp.Known.X.Path
	case ukp.Current.Ok:
		return ukp.Current.X.Path
	}
	panic(ukp)
}

type unknownIter struct {
	conn *sqlutil.Conn
	it   streams.Iterator[unknownFile]
}

func (it *unknownIter) Close() error {
	return it.conn.Close()
}

func (it *unknownIter) Next(ctx context.Context, dst []unknownFile) (int, error) {
	return it.it.Next(ctx, dst)
}

func hasChangedDirAware(a, b *porting.FileInfo) bool {
	if a.Mode.IsDir() || b.Mode.IsDir() {
		return a.Mode != b.Mode
	}
	return porting.HasChanged(a, b)
}

// newUnknownIterator iterates over files which are unknown to the database.
func (wc *WC) newUnknownIterator(db *porting.DB, fsys posixfs.FS, spans []Span) streams.Iterator[unknownFile] {
	dbit := streams.NewPeeker(streams.NewFilter(db.NewInfoIterator(), func(ent porting.FileInfo) bool {
		if strings.HasPrefix(ent.Path, ".got") {
			return false
		}
		return spansContain(spans, ent.Path)
	}), nil)
	fsit := streams.NewPeeker(porting.NewFSInfoIter(fsys), nil)
	join := streams.NewOJoiner(dbit, fsit, func(left porting.FileInfo, right FileInfo) int {
		return strings.Compare(left.Path, right.Path)
	})
	diff := streams.NewFilter(join, func(x streams.OJoined[porting.FileInfo, FileInfo]) bool {
		switch {
		case x.Left.Ok != x.Right.Ok:
			// path only exists in 1
			return true
		case x.Left.Ok && x.Right.Ok:
			return hasChangedDirAware(&x.Left.X, &x.Right.X)
		}
		return false
	})
	return streams.NewMap(diff, func(dst *unknownFile, src streams.OJoined[porting.FileInfo, porting.FileInfo]) {
		*dst = unknownFile{
			Known:   src.Left,
			Current: src.Right,
		}
	})
}
