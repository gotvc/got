package gotiofs

import (
	"fmt"
	"maps"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/gottests"
	"github.com/gotvc/got/src/gotwc"
	"github.com/gotvc/got/src/internal/gotcore"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestFS(t *testing.T) {
	type testCase map[string]string
	tcs := []testCase{
		{"a.txt": "hello"},
		{
			"a.txt": "hello",
			"b.txt": "hello",
			"c.txt": "hello",
		},
		{
			"a.txt":         "hello1",
			"subdir1/b.txt": "hello2",
			"subdir2/c.txt": "hello3",
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			ctx := testutil.Context(t)
			s := gottests.NewSite(t)

			s.CreateMark(gotrepo.FQM{Name: "master"})
			for p, content := range tc {
				s.CreateFile(p, []byte(content))
				s.Add(p)
			}
			s.Commit(gotwc.CommitParams{})

			require.NoError(t, s.Repo.ViewSnapshot(ctx, gotcore.SnapExpr_Mark{Name: "master"}, func(vctx *gotcore.ViewCtx) error {
				fsys := New(ctx, vctx)
				ps := slices.Collect(maps.Keys(tc))
				return fstest.TestFS(fsys, ps...)
			}))
		})
	}
}
