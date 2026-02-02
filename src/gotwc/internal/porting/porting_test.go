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
			root := makeGotFS(t, s, tt.InGot)

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

func makeGotFS(t testing.TB, s stores.RW, ents []FileEntry) gotfs.Root {
	t.Helper()
	cfg := gotcore.DefaultConfig(true)
	mach := gotcore.GotFS(cfg)
	ctx := testutil.Context(t)
	root, err := mach.NewEmpty(ctx, s, 0o755)
	require.NoError(t, err)
	for _, ent := range ents {
		if ent.Mode.IsDir() {
			root, err = mach.MkdirAll(ctx, s, *root, ent.Path)
			require.NoError(t, err)
			continue
		}
		parent := filepath.Dir(ent.Path)
		if parent != "." && parent != "" {
			root, err = mach.MkdirAll(ctx, s, *root, parent)
			require.NoError(t, err)
		}
		root, err = mach.PutFile(ctx, [2]stores.RW{s, s}, *root, ent.Path, strings.NewReader(ent.Data))
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
