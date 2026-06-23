package gotcmd

import (
	"context"
	"crypto/rand"
	"encoding/hex"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/bcns"
	"github.com/gotvc/got/src/gotns"
	"github.com/gotvc/got/src/gotrepo"
	"go.brendoncarroll.net/star"
)

var blobcacheCmd = star.NewDir(
	star.Metadata{Short: "manage blobcache"},
	map[string]star.Command{
		"create-space": bcCreateSpaceCmd,
	},
)

var bcCreateSpaceCmd = star.Command{
	Metadata: star.Metadata{Short: "create a space volume. Does not modify repo config"},
	Pos:      []star.Positional{volNameParam},
	F: func(c star.Context) error {
		ctx := c.Context
		bc := bcclient.NewClientFromEnv()
		bcns, err := bcclient.NewNSClientFromEnv()
		if err != nil {
			return err
		}
		p := volNameParam.Load(c)
		volh, err := bcns.CreateVolume(ctx, p, gotns.SpaceVolumeSpec())
		if err != nil {
			return err
		}
		c.Printf("created volume at %s\n", p)
		u, err := bcsdk.URLFor(ctx, bc, volh)
		if err != nil {
			return err
		}
		var secret [32]byte
		rand.Read(secret[:])
		c.Printf("URL: %v\n", u)
		c.Printf("SECRET: %v\n", hex.EncodeToString(secret[:]))
		return err
	},
}

// createVol create a new volume according to spec at volname in the namespace
func createVol(ctx context.Context, svc blobcache.Service, volName string, spec blobcache.VolumeSpec) (blobcache.Handle, error) {
	root, err := bcclient.EnvNSRoot()
	if err != nil {
		return blobcache.Handle{}, err
	}
	nsc := bcns.NewClient(svc, root)
	return nsc.CreateVolume(ctx, volName, spec)
}

func createRepoVol(ctx context.Context, svc blobcache.Service, volName string) (blobcache.Handle, error) {
	return createVol(ctx, svc, volName, gotrepo.RepoVolumeSpec(false))
}

func createNSVol(ctx context.Context, svc blobcache.Service, volName string) (blobcache.Handle, error) {
	return createVol(ctx, svc, volName, gotns.SpaceVolumeSpec())
}
