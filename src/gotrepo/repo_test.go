package gotrepo

import (
	"testing"

	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestRepoInit(t *testing.T) {
	t.Parallel()
	dirpath := t.TempDir()
	t.Log("testing in", dirpath)
	r := testutil.OpenRoot(t, dirpath)
	require.NoError(t, Init(r, DefaultConfig()))
	repo, err := Open(r)
	require.NoError(t, err)
	require.NotNil(t, repo)
}
