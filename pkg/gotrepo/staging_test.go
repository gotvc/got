package gotrepo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStaging(t *testing.T) {
	t.Parallel()
	repo := newTestRepo(t)
	ops := listStaging(t, repo)
	require.Len(t, ops, 0)
}

func listStaging(t testing.TB, x *Repo) (ret []FileOperation) {
	ctx := context.Background()
	err := x.ForEachStaging(ctx, func(p string, op FileOperation) error {
		ret = append(ret, op)
		return nil
	})
	require.NoError(t, err)
	return ret
}
