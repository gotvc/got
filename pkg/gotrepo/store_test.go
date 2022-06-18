package gotrepo

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/cadata/fsstore"
	"github.com/brendoncarroll/go-state/cadata/storetest"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestStore(t *testing.T) {
	storetest.TestStore(t, func(t testing.TB) cadata.Store {
		dirpath := t.TempDir()
		dbPath := filepath.Join(dirpath, "bolt.db")
		blobPath := filepath.Join(dirpath, "blobs")

		s := fsstore.New(posixfs.NewDirFS(blobPath), cadata.DefaultHash, cadata.DefaultMaxSize)
		db, err := bolt.Open(dbPath, 0o644, &bolt.Options{NoSync: true})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, db.Close())
		})
		sm := newStoreManager(s, db)
		sid, err := sm.Create(context.TODO())
		require.NoError(t, err)
		t.Log("created store", sid)
		return sm.Open(sid)
	})
}
