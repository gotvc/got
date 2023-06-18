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

	"github.com/gotvc/got/pkg/testutil"
)

func TestStore(t *testing.T) {
	storetest.TestStore(t, func(t testing.TB) cadata.Store {
		dirpath := t.TempDir()
		blobPath := filepath.Join(dirpath, "blobs")

		s := fsstore.New(posixfs.NewDirFS(blobPath), cadata.DefaultHash, cadata.DefaultMaxSize)
		db := testutil.NewTestBadger(t)
		sm := newStoreManager(s, db)
		sid, err := sm.Create(context.TODO())
		require.NoError(t, err)
		t.Log("created store", sid)
		return sm.Open(sid)
	})
}
