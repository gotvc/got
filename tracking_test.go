package got

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestTracker(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := bolt.Open(dbPath, 0o644, nil)
	require.NoError(t, err)

	tr := newTracker(db, []string{bucketTracker})
	err = tr.Track(ctx, "test path")
	require.NoError(t, err)
	err = tr.ForEach(ctx, func(p string) error {
		require.Equal(t, "test path", p)
		return nil
	})
	require.NoError(t, err)
	tr.Untrack(ctx, "test path")
	require.NoError(t, err)
	err = tr.ForEach(ctx, func(p string) error {
		require.NotEqual(t, "test path", p)
		return nil
	})
	require.NoError(t, err)
}
