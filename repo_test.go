package got

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRepoInit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dirpath := t.TempDir()
	t.Log("testing in", dirpath)
	require.NoError(t, InitRepo(dirpath))
	repo, err := OpenRepo(dirpath)
	require.NoError(t, err)
	require.NotNil(t, repo)

	name, vol, err := repo.GetActiveVolume(ctx)
	require.NoError(t, err)
	require.Equal(t, nameMaster, name)
	require.NotNil(t, vol)
}
