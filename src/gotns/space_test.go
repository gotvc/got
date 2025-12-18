package gotns

import (
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"blobcache.io/blobcache/src/schema/schematests"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/volumes"
	"github.com/gotvc/got/src/marks"
)

func TestSpace(t *testing.T) {
	marks.TestSpace(t, func(t testing.TB) marks.Space {
		spec := DefaultVolumeSpec()
		bc, volh := schematests.Setup(t, map[blobcache.SchemaName]schema.Constructor{
			"": schema.NoneConstructor,
		}, *spec.Local)
		vol := &volumes.Blobcache{Service: bc, Handle: volh}
		dmach := gdat.NewMachine()
		kvmach := gotkv.NewMachine(1<<13, 1<<18)
		return &Space{
			Volume: vol,
			DMach:  dmach,
			KVMach: &kvmach,
		}
	})
}
