package got

import (
	"testing"

	"github.com/brendoncarroll/got/pkg/branches"
	"github.com/stretchr/testify/require"
)

func TestRealm(t *testing.T) {
	t.Parallel()
	branches.TestRealm(t, func(t testing.TB) branches.Realm {
		dir := t.TempDir()
		err := InitRepo(dir)
		require.NoError(t, err)
		r, err := OpenRepo(dir)
		require.NoError(t, err)
		return r.specDir
	})
}
