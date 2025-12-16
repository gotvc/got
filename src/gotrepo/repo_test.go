package gotrepo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRepoInit(t *testing.T) {
	t.Parallel()
	dirpath := t.TempDir()
	t.Log("testing in", dirpath)
	require.NoError(t, Init(dirpath, DefaultConfig()))
	repo, err := Open(dirpath)
	require.NoError(t, err)
	require.NotNil(t, repo)
}
