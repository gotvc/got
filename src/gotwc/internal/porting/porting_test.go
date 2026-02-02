package porting

import (
	"context"
	"io"
	"io/fs"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotwc/internal/dbmig"
	"github.com/gotvc/got/src/gotwc/internal/migrations"
	"github.com/gotvc/got/src/gotwc/internal/sqlutil"
	"github.com/gotvc/got/src/internal/gotcore"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
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
			db := NewDB(conn, paramHash)

			for _, info := range tt.InDB {
				require.NoError(t, db.PutInfo(ctx, info))
			}
			root := makeGotFS(t, mach, s, tt.InGot)

			exp := NewExporter(mach, db, fsys, func(string) bool { return true })
			err := exp.ExportPath(ctx, s, s, root, tt.ExportPath)
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

func TestImporterReimportOnChange(t *testing.T) {
	tests := []struct {
		name    string
		initial string
		updated string
	}{
		{
			name:    "content change triggers reimport",
			initial: "one",
			updated: "two two",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := testutil.Context(t)

			fsys := posixfs.NewDirFS(t.TempDir())
			require.NoError(t, posixfs.PutFile(ctx, fsys, "a.txt", 0o644, strings.NewReader(tt.initial)))

			cfg := gotcore.DefaultConfig(false)
			conn, paramHash := newTestDB(t, ctx, cfg)
			mach := gotcore.GotFS(cfg)
			ms := stores.NewMem()
			ds := stores.NewMem()
			db := NewDB(conn, paramHash)
			imp := NewImporter(mach, db, [2]stores.RW{ds, ms})

			root1, err := imp.ImportFile(ctx, fsys, "a.txt")
			require.NoError(t, err)
			require.Equal(t, tt.initial, readFileFromRoot(t, ctx, mach, ms, ds, root1, ""))

			require.NoError(t, posixfs.PutFile(ctx, fsys, "a.txt", 0o644, strings.NewReader(tt.updated)))
			root2, err := imp.ImportFile(ctx, fsys, "a.txt")
			require.NoError(t, err)
			require.Equal(t, tt.updated, readFileFromRoot(t, ctx, mach, ms, ds, root2, ""))

			require.False(t, gotfs.Equal(*root1, *root2))
		})
	}
}

func TestByGotPersistence(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, ctx context.Context, fsys posixfs.FS, mach *gotfs.Machine, db *DB, s stores.RW) (string, error)
		want  bool
	}{
		{
			name: "export sets by_got",
			setup: func(t *testing.T, ctx context.Context, fsys posixfs.FS, mach *gotfs.Machine, db *DB, s stores.RW) (string, error) {
				root := makeGotFS(t, mach, s, []FileEntry{
					{Path: "export.txt", Mode: 0o644, Data: "data"},
				})
				exp := NewExporter(mach, db, fsys, func(string) bool { return true })
				return "export.txt", exp.ExportPath(ctx, s, s, root, "export.txt")
			},
			want: true,
		},
		{
			name: "export dir sets by_got",
			setup: func(t *testing.T, ctx context.Context, fsys posixfs.FS, mach *gotfs.Machine, db *DB, s stores.RW) (string, error) {
				root := makeGotFS(t, mach, s, []FileEntry{
					{Path: "exportdir", Mode: 0o755 | fs.ModeDir},
				})
				exp := NewExporter(mach, db, fsys, func(string) bool { return true })
				return "exportdir", exp.ExportPath(ctx, s, s, root, "exportdir")
			},
			want: true,
		},
		{
			name: "import sets by_got false",
			setup: func(t *testing.T, ctx context.Context, fsys posixfs.FS, mach *gotfs.Machine, db *DB, s stores.RW) (string, error) {
				if err := posixfs.PutFile(ctx, fsys, "import.txt", 0o644, strings.NewReader("data")); err != nil {
					return "import.txt", err
				}
				imp := NewImporter(mach, db, [2]stores.RW{s, s})
				_, err := imp.ImportFile(ctx, fsys, "import.txt")
				return "import.txt", err
			},
			want: false,
		},
		{
			name: "import dir sets by_got false",
			setup: func(t *testing.T, ctx context.Context, fsys posixfs.FS, mach *gotfs.Machine, db *DB, s stores.RW) (string, error) {
				if err := posixfs.MkdirAll(fsys, "importdir", 0o755); err != nil {
					return "importdir", err
				}
				imp := NewImporter(mach, db, [2]stores.RW{s, s})
				_, err := imp.ImportPath(ctx, fsys, "importdir")
				return "importdir", err
			},
			want: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := testutil.Context(t)

			fsys := posixfs.NewDirFS(t.TempDir())
			cfg := gotcore.DefaultConfig(false)
			conn, paramHash := newTestDB(t, ctx, cfg)
			mach := gotcore.GotFS(cfg)
			s := stores.NewMem()
			db := NewDB(conn, paramHash)

			p, err := tt.setup(t, ctx, fsys, mach, db, s)
			require.NoError(t, err)

			var got FileInfo
			ok, err := db.GetInfo(ctx, p, &got)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, tt.want, got.ByGot)
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
		root, err = fsmach.PutFile(ctx, [2]stores.RW{s, s}, *root, ent.Path, strings.NewReader(ent.Data))
		require.NoError(t, err)
	}
	return *root
}

func readFileFromRoot(t testing.TB, ctx context.Context, mach *gotfs.Machine, ms, ds stores.Reading, root *gotfs.Root, p string) string {
	t.Helper()
	r, err := mach.NewReader(ctx, [2]stores.Reading{ds, ms}, *root, p)
	require.NoError(t, err)
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(data)
}
