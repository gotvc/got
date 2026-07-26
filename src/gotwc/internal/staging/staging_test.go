package staging

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/volumes"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/state/posixfs"
	"go.etcd.io/bbolt"
)

type testFile struct {
	Path string
	Data string
}

type stageAction struct {
	Kind string
	Path string
}

func TestForEachStaged_Table(t *testing.T) {
	type TestCase struct {
		Name       string
		BaseFiles  []testFile
		Worktree   []testFile
		Actions    []stageAction
		NoBase     bool
		WantByPath map[string]string
	}

	tcs := []TestCase{
		{
			Name:       "create_with_no_base",
			Worktree:   []testFile{{Path: "new.txt", Data: "hello"}},
			Actions:    []stageAction{{Kind: "add", Path: "new.txt"}},
			NoBase:     true,
			WantByPath: map[string]string{"new.txt": "CREATE"},
		},
		{
			Name:       "modify_existing",
			BaseFiles:  []testFile{{Path: "a.txt", Data: "old"}},
			Worktree:   []testFile{{Path: "a.txt", Data: "new"}},
			Actions:    []stageAction{{Kind: "add", Path: "a.txt"}},
			WantByPath: map[string]string{"a.txt": "MODIFY"},
		},
		{
			Name:       "delete_existing",
			BaseFiles:  []testFile{{Path: "a.txt", Data: "old"}},
			Worktree:   nil,
			Actions:    []stageAction{{Kind: "delete", Path: "a.txt"}},
			WantByPath: map[string]string{"a.txt": "DELETE"},
		},
		{
			Name:       "discard_removes_staged_path",
			Worktree:   []testFile{{Path: "a.txt", Data: "content"}},
			Actions:    []stageAction{{Kind: "add", Path: "a.txt"}, {Kind: "discard", Path: "a.txt"}},
			NoBase:     true,
			WantByPath: map[string]string{},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.Name, func(t *testing.T) {
			ctx := context.Background()
			tx, fsys, ss, base := newTestTx(t, ctx, tc.BaseFiles, tc.Worktree)
			applyActions(t, ctx, tx, fsys, ss, base, tc.Actions)

			var basePtr *gotfs.Root
			if !tc.NoBase {
				basePtr = &base
			}
			got := map[string]string{}
			err := tx.ForEachStaged(ctx, ss, basePtr, func(p string, op FileOperation) error {
				got[p] = opKind(op)
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, tc.WantByPath, got)
		})
	}
}

func TestForEachUntracked_Table(t *testing.T) {
	type TestCase struct {
		Name      string
		BaseFiles []testFile
		Worktree  []testFile
		Actions   []stageAction
		NoBase    bool
		WantPaths []string
	}

	tcs := []TestCase{
		{
			Name:      "new_file_is_untracked",
			Worktree:  []testFile{{Path: "new.txt", Data: "hello"}},
			NoBase:    true,
			WantPaths: []string{"new.txt"},
		},
		{
			Name:      "staged_create_not_untracked",
			Worktree:  []testFile{{Path: "new.txt", Data: "hello"}},
			Actions:   []stageAction{{Kind: "add", Path: "new.txt"}},
			NoBase:    true,
			WantPaths: nil,
		},
		{
			Name:      "tracked_modified_file_not_untracked",
			BaseFiles: []testFile{{Path: "tracked.txt", Data: "old"}},
			Worktree:  []testFile{{Path: "tracked.txt", Data: "new"}},
			WantPaths: nil,
		},
		{
			Name:      "only_new_file_is_untracked",
			BaseFiles: []testFile{{Path: "tracked.txt", Data: "same"}},
			Worktree: []testFile{
				{Path: "tracked.txt", Data: "same"},
				{Path: "new.txt", Data: "hello"},
			},
			WantPaths: []string{"new.txt"},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.Name, func(t *testing.T) {
			ctx := context.Background()
			tx, fsys, ss, base := newTestTx(t, ctx, tc.BaseFiles, tc.Worktree)
			applyActions(t, ctx, tx, fsys, ss, base, tc.Actions)

			var basePtr *gotfs.Root
			if !tc.NoBase {
				basePtr = &base
			}

			var got []string
			err := tx.ForEachUntracked(ctx, fsys, ss, basePtr, func(p string) error {
				got = append(got, p)
				return nil
			})
			require.NoError(t, err)

			sort.Strings(got)
			want := append([]string(nil), tc.WantPaths...)
			sort.Strings(want)
			require.Equal(t, want, got)
		})
	}
}

func TestForEachDirty_Table(t *testing.T) {
	type TestCase struct {
		Name      string
		BaseFiles []testFile
		Worktree  []testFile
		Actions   []stageAction
		Want      map[string]bool
	}

	tcs := []TestCase{
		{
			Name:      "tracked_not_known_is_dirty",
			BaseFiles: []testFile{{Path: "a.txt", Data: "same"}},
			Worktree:  []testFile{{Path: "a.txt", Data: "same"}},
			Want:      map[string]bool{"a.txt": true},
		},
		{
			Name:      "tracked_modified_is_dirty",
			BaseFiles: []testFile{{Path: "a.txt", Data: "old"}},
			Worktree:  []testFile{{Path: "a.txt", Data: "new"}},
			Want:      map[string]bool{"a.txt": true},
		},
		{
			Name:      "tracked_deleted_is_dirty",
			BaseFiles: []testFile{{Path: "a.txt", Data: "old"}},
			Worktree:  nil,
			Want:      map[string]bool{"a.txt": false},
		},
		{
			Name:      "untracked_is_not_dirty",
			BaseFiles: nil,
			Worktree:  []testFile{{Path: "new.txt", Data: "x"}},
			Want:      map[string]bool{},
		},
		{
			Name:      "staged_path_not_dirty",
			BaseFiles: []testFile{{Path: "a.txt", Data: "old"}},
			Worktree:  []testFile{{Path: "a.txt", Data: "new"}},
			Actions:   []stageAction{{Kind: "add", Path: "a.txt"}},
			Want:      map[string]bool{},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.Name, func(t *testing.T) {
			ctx := context.Background()
			tx, fsys, ss, base := newTestTx(t, ctx, tc.BaseFiles, tc.Worktree)
			applyActions(t, ctx, tx, fsys, ss, base, tc.Actions)

			got := map[string]bool{}
			err := tx.ForEachDirty(ctx, fsys, ss, base, func(df DirtyFile) error {
				got[df.Path] = df.Exists
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, tc.Want, got)
		})
	}
}

func opKind(op FileOperation) string {
	switch {
	case op.Create != nil:
		return "CREATE"
	case op.Modify != nil:
		return "MODIFY"
	case op.Delete != nil:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

func applyActions(t *testing.T, ctx context.Context, tx *Tx, fsys posixfs.FS, ss gotfs.RO, base gotfs.Root, actions []stageAction) {
	t.Helper()
	for _, action := range actions {
		switch action.Kind {
		case "add":
			require.NoError(t, tx.Add(ctx, fsys, action.Path))
		case "put":
			require.NoError(t, tx.Put(ctx, fsys, action.Path))
		case "delete":
			require.NoError(t, tx.Delete(ctx, fsys, ss, base, action.Path))
		case "discard":
			require.NoError(t, tx.Discard(ctx, action.Path))
		default:
			t.Fatalf("unknown action kind %q", action.Kind)
		}
	}
}

func newTestTx(t *testing.T, ctx context.Context, baseFiles, worktree []testFile) (*Tx, posixfs.FS, gotfs.RO, gotfs.Root) {
	t.Helper()

	workDir := t.TempDir()
	writeWorktree(t, workDir, worktree)
	fsys := posixfs.NewDirFS(workDir)

	repoStore := stores.NewMem()
	fsmach := gotfs.NewMachine(gotfs.Params{})
	base := makeBaseRoot(t, ctx, &fsmach, repoStore, baseFiles)
	ss := gotfs.RO{Metadata: repoStore, Data: repoStore}

	dbPath := filepath.Join(t.TempDir(), "stage.db")
	bdb, err := bbolt.Open(dbPath, 0o600, &bbolt.Options{NoSync: true, NoFreelistSync: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })
	btx, err := bdb.Begin(true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = btx.Rollback() })

	vol := volumes.NewMemory(blobcache.HashAlgo_BLAKE2b_256, 1<<24)
	voltx, err := vol.BeginTx(ctx, volumes.TxParams{Modify: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = voltx.Abort(ctx) })

	paramHash := [32]byte{1}
	tx := New(Env{
		Tx:        btx,
		VolTx:     voltx,
		GotFS:     &fsmach,
		ParamHash: &paramHash,
	})
	return tx, fsys, ss, base
}

func makeBaseRoot(t *testing.T, ctx context.Context, fsmach *gotfs.Machine, s stores.RW, files []testFile) gotfs.Root {
	t.Helper()
	root, err := fsmach.NewEmpty(ctx, s, 0o755)
	require.NoError(t, err)
	for _, file := range files {
		parent := path.Dir(file.Path)
		if parent != "." && parent != "" {
			root, err = fsmach.MkdirAll(ctx, s, root, parent)
			require.NoError(t, err)
		}
		root, err = fsmach.PutFile(ctx, gotfs.RW{Metadata: s, Data: s}, root, file.Path, strings.NewReader(file.Data))
		require.NoError(t, err)
	}
	return root
}

func writeWorktree(t *testing.T, dir string, files []testFile) {
	t.Helper()
	for _, file := range files {
		p := filepath.Join(dir, file.Path)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte(file.Data), 0o644))
	}
}
