package gotfs

import (
	"io/fs"
	"strconv"
	"testing"

	"github.com/brendoncarroll/go-exp/streams"
	"github.com/gotvc/got/pkg/stores"
	"github.com/gotvc/got/pkg/testutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

func TestDiffer(t *testing.T) {
	ctx := testutil.Context(t)
	op := NewOperator()

	tcs := []struct {
		Left     memFS
		Right    memFS
		Expected []DeltaEntry
	}{
		{
			Left:     nil,
			Right:    nil,
			Expected: nil,
		},
		{
			Left: map[string]memFile{
				"a.txt": {Mode: 0o644},
			},
			Expected: []DeltaEntry{
				{Path: "a.txt", Delete: &struct{}{}},
			},
		},
		{
			Right: map[string]memFile{
				"a.txt": {Mode: 0o644},
			},
			Expected: []DeltaEntry{
				{Path: "a.txt", PutInfo: &Info{Mode: 0o644}},
			},
		},
	}
	for i, tc := range tcs {
		tc := tc
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			s := stores.NewMem()
			lb := op.NewBuilder(ctx, s, s)
			left := buildFS(t, lb, tc.Left)
			rb := op.NewBuilder(ctx, s, s)
			right := buildFS(t, rb, tc.Right)

			d := op.NewDiffer(s, left, right)
			actual, err := streams.Collect[DeltaEntry](ctx, d, 100)
			require.NoError(t, err)
			require.Equal(t, len(tc.Expected), len(actual))
			for i := range tc.Expected {
				requireEqualDeltas(t, tc.Expected[i], actual[i])
			}
		})
	}
}

type memFile struct {
	Mode uint32
	Data []byte
}

type memFS = map[string]memFile

func buildFS(t testing.TB, b *Builder, m memFS) Root {
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
