package gotrepo

import (
	"testing"

	"github.com/gotvc/got/pkg/branches"
	"github.com/stretchr/testify/require"
)

func TestSpace(t *testing.T) {
	t.Parallel()
	branches.TestSpace(t, func(t testing.TB) branches.Space {
		dir := t.TempDir()
		err := Init(dir)
		require.NoError(t, err)
		r, err := Open(dir)
		require.NoError(t, err)
		return r.specDir
	})
}
