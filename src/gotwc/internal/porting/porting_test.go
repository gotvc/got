package porting

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotwc/internal/dbmig"
	"github.com/gotvc/got/src/gotwc/internal/migrations"
	"github.com/gotvc/got/src/gotwc/internal/sqlutil"
	"github.com/gotvc/got/src/internal/gotcore"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/state/posixfs"
)

type FileEntry struct {
	Path string
	Mode fs.FileMode
	Data string
}

func TestExport(t *testing.T) {
	type testCase struct {
		Name string
		// InFS are the files
		InFS []FileEntry
		// InDB are the FileInfos in the DB before the Export
		InDB []FileInfo
		// RootEntries are the entries to build into GotFS before export.
		InGot []FileEntry
		// ExportPath is the path to export from GotFS.
		ExportPath string

		// Err is the expected error from the export operation.
		Err error
	}
	tests := []testCase{
		{
			Name: "file overwrite",
			InFS: []FileEntry{
				{Path: "a.txt", Mode: 0o644, Data: "untracked"},
			},
			InGot: []FileEntry{
				{Path: "a.txt", Mode: 0o644, Data: "got"},
			},
			ExportPath: "a.txt",
			Err: ErrWouldClobber{
				Op:   "write",
				Path: "a.txt",
			},
		},
		{
			Name: "dir delete untracked child",
			InFS: []FileEntry{
				{Path: "dir", Mode: 0o755 | fs.ModeDir},
				{Path: filepath.Join("dir", "untracked.txt"), Mode: 0o644, Data: "untracked"},
			},
			InGot: []FileEntry{
				{Path: "dir", Mode: 0o755 | fs.ModeDir},
			},
			ExportPath: "dir",
			Err: ErrWouldClobber{
				Op:   "delete",
				Path: filepath.Join("dir", "untracked.txt"),
			},
		},
		{
			Name: "export new file",
			InGot: []FileEntry{
				{Path: "new.txt", Mode: 0o644, Data: "data"},
			},
			ExportPath: "new.txt",
		},
		{
			Name: "export new dir",
			InGot: []FileEntry{
				{Path: "newdir", Mode: 0o755 | fs.ModeDir},
			},
			ExportPath: "newdir",
		},
		{
			Name: "replace empty dir with file",
			InFS: []FileEntry{
				{Path: "swap", Mode: 0o755 | fs.ModeDir},
			},
			InGot: []FileEntry{
				{Path: "swap", Mode: 0o644, Data: "file"},
			},
			ExportPath: "swap",
			Err: ErrWouldClobber{
				Op:   "write",
				Path: "swap",
			},
		},
		{
			Name: "file overwrite tracked changed",
			InFS: []FileEntry{
				{Path: "tracked.txt", Mode: 0o644, Data: "on disk"},
			},
			InDB: []FileInfo{
				{Path: "tracked.txt", Mode: 0o644, Size: 1},
			},
			InGot: []FileEntry{
				{Path: "tracked.txt", Mode: 0o644, Data: "got"},
			},
			ExportPath: "tracked.txt",
			Err: ErrWouldClobber{
				Op:   "write",
				Path: "tracked.txt",
			},
		},
		{
			Name: "delete tracked changed child",
			InFS: []FileEntry{
				{Path: "dir", Mode: 0o755 | fs.ModeDir},
				{Path: filepath.Join("dir", "old.txt"), Mode: 0o644, Data: "old"},
			},
			InDB: []FileInfo{
				{Path: filepath.Join("dir", "old.txt"), Mode: 0o644, Size: 0},
			},
			InGot: []FileEntry{
				{Path: "dir", Mode: 0o755 | fs.ModeDir},
			},
			ExportPath: "dir",
			Err: ErrWouldClobber{
				Op:   "delete",
				Path: filepath.Join("dir", "old.txt"),
			},
		},
		{
			Name: "tracked entry without working file",
			InDB: []FileInfo{
				{Path: "tracked.txt", Mode: 0o644, Size: 0},
			},
			InGot: []FileEntry{
				{Path: "tracked.txt", Mode: 0o644, Data: "got"},
			},
			ExportPath: "tracked.txt",
		},
		{
			Name: "unrelated tracked entry",
			InDB: []FileInfo{
				{Path: "other.txt", Mode: 0o644, Size: 0},
			},
			InGot: []FileEntry{
				{Path: "target.txt", Mode: 0o644, Data: "data"},
			},
			ExportPath: "target.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			t.Parallel()
			ctx := testutil.Context(t)

			fsys := posixfs.NewDirFS(t.TempDir())
			for _, ent := range tt.InFS {
				if ent.Mode.IsDir() {
					require.NoError(t, posixfs.MkdirAll(fsys, ent.Path, ent.Mode.Perm()))
					continue
				}
				parent := path.Dir(ent.Path)
				if parent != "." && parent != "" {
					require.NoError(t, posixfs.MkdirAll(fsys, parent, 0o755))
				}
				require.NoError(t, posixfs.PutFile(ctx, fsys, ent.Path, ent.Mode, strings.NewReader(ent.Data)))
			}

			cfg := gotcore.DefaultConfig(true)
			conn, paramHash := newTestDB(t, ctx, cfg)
			mach := gotcore.GotFS(cfg)
			s := stores.NewMem()
			ss := gotfs.RO{s, s}
			db := NewDB(conn, paramHash)

			for _, info := range tt.InDB {
				require.NoError(t, db.PutInfo(ctx, info))
			}
			root := makeGotFS(t, mach, s, tt.InGot)

			exp := NewExporter(mach, db, fsys, func(string) bool { return true })
			err := exp.ExportPath(ctx, ss, root, tt.ExportPath)
			if tt.Err == nil {
				require.NoError(t, err)
				return
			}
			switch expected := tt.Err.(type) {
			case ErrWouldClobber:
				var clobber ErrWouldClobber
				require.ErrorAs(t, err, &clobber)
				require.Equal(t, expected.Op, clobber.Op)
				require.Equal(t, expected.Path, clobber.Path)
			default:
				require.EqualError(t, err, tt.Err.Error())
			}
		})
	}
}

// TestImportPath tests that Importer.ImportPath returns a
// valid gotfs.Root which contains all the imported files.
func TestImportPath(t *testing.T) {
	type testCase struct {
		// FS is the contents of the file system
		FS []FileEntry
		// Path is what is passed to import path
		Path string
	}
	tcs := []testCase{
		{
			FS: []FileEntry{
				{Path: "a.txt", Mode: 0o644, Data: "one"},
				{Path: "b.txt", Mode: 0o644, Data: "two"},
				{Path: "c.txt", Mode: 0o644, Data: "three"},
			},
			Path: "a.txt",
		},
		{
			FS: []FileEntry{
				{Path: "subdir1", Mode: 0o755 | fs.ModeDir},
				{Path: "subdir2", Mode: 0o755 | fs.ModeDir},
				{Path: "subdir3", Mode: 0o755 | fs.ModeDir},
				{Path: "subdir2/a.txt", Mode: 0o644, Data: "one"},
				{Path: "subdir2/b.txt", Mode: 0o644, Data: "two"},
				{Path: "subdir2/c.txt", Mode: 0o644, Data: "three"},
			},
			Path: "subdir2",
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			ctx := testutil.Context(t)

			// setup imp
			dst := stores.NewMem()
			cfg := gotcore.DefaultConfig(false)
			mach := gotcore.GotFS(cfg)
			conn, paramHash := newTestDB(t, ctx, cfg)
			db := NewDB(conn, paramHash)
			imp := NewImporter(mach, db, [2]stores.RW{dst, dst})

			// prepare files on disk
			dir := testutil.OpenRoot(t, t.TempDir())
			writeToFS(t, dir, tc.FS)

			// do import
			fsys := posixfs.NewDirFS(dir.Name())
			root, err := imp.ImportPath(ctx, fsys, tc.Path)
			require.NoError(t, err)
			require.NoError(t, mach.Check(ctx, dst, *root, func(gdat.Ref) error {
				return nil
			}))

			it := mach.NewInfoIterator(dst, *root)
			infos, err := streams.Collect(ctx, it, len(tc.FS))
			require.NoError(t, err)
			for _, ent := range infos {
				p := path.Join(tc.Path, ent.Path)
				finfo, err := dir.Stat(p)
				require.NoError(t, err)
				require.Equal(t, finfo.Mode(), ent.Info.Mode)
			}
		})
	}
}

func newTestDB(t testing.TB, ctx context.Context, cfg gotcore.DSConfig) (*sqlutil.Conn, [32]byte) {
	t.Helper()
	pool := sqlutil.NewTestPool(t)
	conn, err := pool.Take(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		pool.Put(conn)
	})

	require.NoError(t, migrations.EnsureAll(conn, dbmig.ListMigrations()))
	return conn, cfg.Hash()
}

func writeToFS(t testing.TB, dir *os.Root, ents []FileEntry) {
	t.Helper()
	for _, ent := range ents {
		mode := ent.Mode & fs.ModePerm
		if ent.Mode.IsDir() {
			require.NoError(t, dir.Mkdir(ent.Path, mode))
		} else {
			require.NoError(t, dir.WriteFile(ent.Path, []byte(ent.Data), mode))
		}
	}
}

func makeGotFS(t testing.TB, fsmach *gotfs.Machine, s stores.RW, ents []FileEntry) gotfs.Root {
	t.Helper()
	ctx := testutil.Context(t)
	root, err := fsmach.NewEmpty(ctx, s, 0o755)
	require.NoError(t, err)
	for _, ent := range ents {
		if ent.Mode.IsDir() {
			root, err = fsmach.MkdirAll(ctx, s, *root, ent.Path)
			require.NoError(t, err)
			continue
		}
		parent := filepath.Dir(ent.Path)
		if parent != "." && parent != "" {
			root, err = fsmach.MkdirAll(ctx, s, *root, parent)
			require.NoError(t, err)
		}
		root, err = fsmach.PutFile(ctx, gotfs.RW{s, s}, *root, ent.Path, strings.NewReader(ent.Data))
		require.NoError(t, err)
	}
	return *root
}
