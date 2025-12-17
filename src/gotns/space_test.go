package gotns

import (
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"blobcache.io/blobcache/src/schema/schematests"
	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/gotvc/got/src/internal/volumes"
	"github.com/stretchr/testify/require"
)

func TestSpace(t *testing.T) {
	branches.TestSpace(t, func(t testing.TB) branches.Space {
		spec := DefaultVolumeSpec()
		bc, volh := schematests.Setup(t, map[blobcache.SchemaName]schema.Constructor{
			"": schema.NoneConstructor,
		}, *spec.Local)
		vol := &volumes.Blobcache{Service: bc, Handle: volh}
		ctx := testutil.Context(t)
		dmach := gdat.NewMachine()
		kvmach := gotkv.NewMachine(1<<13, 1<<18)
		tx, err := BeginTx(ctx, dmach, &kvmach, vol, true)
		require.NoError(t, err)
		return &Space{
			Tx: tx,
		}
	})
}
