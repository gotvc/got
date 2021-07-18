package gotrepo

import (
	"testing"

	"github.com/gotvc/got/pkg/branches"
	"github.com/stretchr/testify/require"
)

func TestRealm(t *testing.T) {
	t.Parallel()
	branches.TestRealm(t, func(t testing.TB) branches.Realm {
		dir := t.TempDir()
		err := Init(dir)
		require.NoError(t, err)
		r, err := Open(dir)
		require.NoError(t, err)
		return r.specDir
	})
}
