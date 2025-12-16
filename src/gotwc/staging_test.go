package gotwc

import (
	"testing"

	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestStaging(t *testing.T) {
	t.Parallel()
	wc := newTestWC(t)
	ops := listStaging(t, wc)
	require.Len(t, ops, 0)
}

func listStaging(t testing.TB, x *WC) (ret []FileOperation) {
	ctx := testutil.Context(t)
	err := x.forEachStaging(ctx, func(p string, op FileOperation) error {
		ret = append(ret, op)
		return nil
	})
	require.NoError(t, err)
	return ret
}
