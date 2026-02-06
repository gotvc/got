package gotwc

import (
	"io/fs"
	"testing"
	"time"

	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/gotwc/internal/porting"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/state/posixfs"
	"go.brendoncarroll.net/tai64"
)

func TestUnknownIteratorFiltersTracking(t *testing.T) {
	tests := []struct {
		name     string
		tracking []string
		entries  []string
		want     []string
	}{
		{
			name:     "filters by tracking and dot-got",
			tracking: []string{"a/"},
			entries:  []string{"a/file.txt", "b/file.txt", ".got/config"},
			want:     []string{"a/file.txt"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := testutil.Context(t)
			wc := newTestWC(t, false)

			for _, prefix := range tt.tracking {
				require.NoError(t, wc.Track(ctx, PrefixSpan(prefix)))
			}

			head, err := wc.GetHead()
			require.NoError(t, err)
			info, err := wc.repo.InspectMark(ctx, gotrepo.FQM{Name: head})
			require.NoError(t, err)

			conn, err := wc.db.Take(ctx)
			require.NoError(t, err)
			defer wc.db.Put(conn)

			db := porting.NewDB(conn, info.Config.Hash())
			for _, p := range tt.entries {
				require.NoError(t, db.PutInfo(ctx, porting.FileInfo{
					Path:       p,
					Mode:       0o644,
					ModifiedAt: tai64.FromGoTime(time.Unix(1, 0)),
					Size:       1,
				}))
			}

			fsys := posixfs.NewDirFS(t.TempDir())
			spans, err := wc.ListSpans(ctx)
			require.NoError(t, err)
			it := wc.newUnknownIterator(db, fsys, spans)

			var got []string
			require.NoError(t, streams.ForEach(ctx, it, func(uk unknownFile) error {
				got = append(got, uk.Path())
				return nil
			}))

			require.Equal(t, tt.want, got)
		})
	}
}

func TestHasChangedDirAware(t *testing.T) {
	mod1 := time.Unix(1, 0)
	mod2 := time.Unix(2, 0)

	mkInfo := func(mode fs.FileMode, mod time.Time, size int64) porting.FileInfo {
		return porting.FileInfo{
			Mode:       mode,
			ModifiedAt: tai64.FromGoTime(mod),
			Size:       size,
		}
	}

	tests := []struct {
		name string
		a    porting.FileInfo
		b    porting.FileInfo
		want bool
	}{
		{
			name: "dir modtime change ignored",
			a:    mkInfo(0o755|fs.ModeDir, mod1, 0),
			b:    mkInfo(0o755|fs.ModeDir, mod2, 10),
			want: false,
		},
		{
			name: "dir mode change detected",
			a:    mkInfo(0o755|fs.ModeDir, mod1, 0),
			b:    mkInfo(0o700|fs.ModeDir, mod1, 0),
			want: true,
		},
		{
			name: "dir replaced by file",
			a:    mkInfo(0o755|fs.ModeDir, mod1, 0),
			b:    mkInfo(0o644, mod1, 1),
			want: true,
		},
		{
			name: "file replaced by dir",
			a:    mkInfo(0o644, mod1, 1),
			b:    mkInfo(0o755|fs.ModeDir, mod1, 0),
			want: true,
		},
		{
			name: "file modtime change detected",
			a:    mkInfo(0o644, mod1, 1),
			b:    mkInfo(0o644, mod2, 1),
			want: true,
		},
		{
			name: "file unchanged",
			a:    mkInfo(0o644, mod1, 1),
			b:    mkInfo(0o644, mod1, 1),
			want: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, hasChangedDirAware(&tt.a, &tt.b))
		})
	}
}
