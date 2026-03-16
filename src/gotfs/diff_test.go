package gotfs

import (
	"io/fs"
	"strconv"
	"testing"

	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/exp/maybe"
	"go.brendoncarroll.net/exp/streams"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

type (
	MemFS   = testutil.MemFS
	MemFile = testutil.MemFile
)

func TestInfoDiffer(t *testing.T) {
	ctx := testutil.Context(t)
	ag := NewMachine(Params{})

	tcs := []struct {
		Left     MemFS
		Right    MemFS
		Expected []InfoDiff
	}{
		{
			Left:     nil,
			Right:    nil,
			Expected: nil,
		},
		{
			Left: map[string]MemFile{
				"a.txt": {Mode: 0o644},
			},
			Expected: []InfoDiff{
				{Path: "a.txt", Left: maybe.Just(Info{Mode: 0o644})},
			},
		},
		{
			Right: map[string]MemFile{
				"a.txt": {Mode: 0o644},
			},
			Expected: []InfoDiff{
				{Path: "a.txt", Right: maybe.Just(Info{Mode: 0o644})},
			},
		},
	}
	for i, tc := range tcs {
		tc := tc
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			s := stores.NewMem()
			ss := RW{s, s}
			lb := ag.NewBuilder(ctx, ss)
			left := buildFS(t, lb, tc.Left)
			rb := ag.NewBuilder(ctx, ss)
			right := buildFS(t, rb, tc.Right)

			d := ag.NewInfoDiffer(s, left, right)
			actual, err := streams.Collect[InfoDiff](ctx, &d, 100)
			require.NoError(t, err)
			require.Equal(t, len(tc.Expected), len(actual))
			for i := range tc.Expected {
				require.Equal(t, tc.Expected[i].Path, actual[i].Path)
			}
		})
	}
}

func buildFS(t testing.TB, b *Builder, m testutil.MemFS) Root {
	ks := maps.Keys(m)
	slices.Sort(ks)

	require.NoError(t, b.Mkdir("", 0o755))
	for _, k := range ks {
		f := m[k]
		if mode := fs.FileMode(f.Mode); mode.IsDir() {
			require.NoError(t, b.Mkdir(k, mode))
		} else {
			require.NoError(t, b.BeginFile(k, fs.FileMode(f.Mode)))
			_, err := b.Write(m[k].Data)
			require.NoError(t, err)
		}
	}
	root, err := b.Finish()
	require.NoError(t, err)
	return *root
}
