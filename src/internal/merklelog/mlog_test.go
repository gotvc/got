package merklelog

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/state/cadata"
)

func TestGet(t *testing.T) {
	for i := 0; i < 10; i++ {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			xs := make([]int, i)
			for i := range xs {
				xs[i] = i
			}

			rws := stores.NewMem()
			st := makeState(t, rws, xs)
			ctx := testutil.Context(t)
			for i := range xs {
				actual, err := Get(ctx, rws, st, Pos(i))
				require.NoError(t, err)
				expected := cidFromInt(rws.Hash, xs[i])
				require.Equal(t, expected, actual)
			}
		})
	}
}

func TestIncludes(t *testing.T) {
	tcs := []struct {
		A []int
		B []int
	}{
		{
			A: []int{},
			B: []int{},
		},
		{
			A: []int{1},
			B: []int{},
		},
		{
			A: []int{1, 2},
			B: []int{1},
		},
		{
			A: []int{1, 2, 3},
			B: []int{1, 2},
		},
		{
			A: []int{1, 2, 3},
			B: []int{1, 2, 3},
		},
		{
			A: []int{1, 2, 3},
			B: []int{1, 2, 3, 4},
		},
		{
			A: []int{1, 2, 3},
			B: []int{4, 5, 6},
		},
	}

	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			s := stores.NewMem()
			astate := makeState(t, s, tc.A)
			bstate := makeState(t, s, tc.B)
			expected := hasPrefix(tc.A, tc.B)
			includes, err := Includes(context.Background(), s, astate, bstate)
			require.NoError(t, err)
			require.Equal(t, expected, includes)
		})
	}
}

func makeState(t testing.TB, s stores.RW, xs []int) State {
	ctx := context.Background()
	st := State{}
	for _, x := range xs {
		cid, err := s.Post(ctx, fmt.Append(nil, x))
		if err != nil {
			t.Fatal(err)
		}
		if err := st.Append(ctx, s, cid); err != nil {
			t.Fatal(err)
		}
	}
	return st
}

func cidFromInt(hf cadata.HashFunc, i int) blobcache.CID {
	return hf(fmt.Append(nil, i))
}

// hasPrefix returns true if a has a prefix of b.
func hasPrefix[T comparable, S ~[]T](a, b S) bool {
	if len(b) > len(a) {
		return false
	}
	return slices.Equal(a[:len(b)], b)
}
