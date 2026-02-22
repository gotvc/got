package gotcmd

import (
	"context"
	"encoding/json"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gotns"
	"github.com/gotvc/got/src/gotrepo"
	"go.brendoncarroll.net/star"
)

var blobcacheCmd = star.NewDir(
	star.Metadata{Short: "manage blobcache"},
	map[string]star.Command{
		"gotns-spec":   gotnsSpecCmd,
		"gotrepo-spec": gotrepoSpecCmd,
		"mkrepo":       mkrepoCmd,
	},
)

var gotnsSpecCmd = star.Command{
	Metadata: star.Metadata{
		Short: "prints a Volume spec suitable to hold a gotns Space",
	},
	F: func(c star.Context) error {
		spec := gotns.DefaultVolumeSpec()
		if err := prettyPrintJSON(c.StdOut, spec); err != nil {
			return err
		}
		c.Printf("\n")
		return nil
	},
}

var gotrepoSpecCmd = star.Command{
	Metadata: star.Metadata{
		Short: "prints a Volume spec suitable to hold a Repo",
	},
	F: func(c star.Context) error {
		spec := gotrepo.RepoVolumeSpec(false)
		if err := prettyPrintJSON(c.StdOut, spec); err != nil {
			return err
		}
		c.Printf("\n")
		return nil
	},
}

var mkrepoCmd = star.Command{
	Metadata: star.Metadata{
		Short: "create a new Volume suitable for use as Repo root",
	},
	Pos: []star.Positional{volNameParam},
	F: func(c star.Context) error {
		ctx := c.Context
		svc := bcclient.NewClientFromEnv()
		volName := volNameParam.Load(c)
		volh, err := createRepoVol(ctx, svc, volName)
		if err != nil {
			return err
		}
		spec := gotrepo.RepoVolumeSpec(false)
		specJSON, err := json.Marshal(spec)
		if err != nil {
			return err
		}
		c.Printf("Successfully created a new volume in the ns root\n")
		c.Printf("OID: %v\n", volh.OID)
		c.Printf("INFO: %s\n", prettifyJSON(specJSON))

		c.Printf("\nNEXT:\n")
		c.Printf("  |> If you are accessing blobcache over the local socket, then you should have access already.\n")
		c.Printf("  |> Provide this volume in a call to got init like this:\n")
		c.Printf("  |> got init --blobcache from-env --volume %v\n", volh.OID)

		return nil
	},
}

var volNameParam = star.Required[string]{
	ID:       "vol-name",
	Parse:    star.ParseString,
	ShortDoc: "the name in the namespace to use for the new volume",
}

// createVol create a new volume according to spec at volname in the namespace
func createVol(ctx context.Context, svc blobcache.Service, volName string, spec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	nsh, nsc, err := bcclient.OpenNSRoot(ctx, svc)
	if err != nil {
		return nil, err
	}
	defer svc.Drop(ctx, *nsh)
	return nsc.CreateAt(ctx, *nsh, volName, spec)
}

func createRepoVol(ctx context.Context, svc blobcache.Service, volName string) (*blobcache.Handle, error) {
	return createVol(ctx, svc, volName, gotrepo.RepoVolumeSpec(false))
}

func createNSVol(ctx context.Context, svc blobcache.Service, volName string) (*blobcache.Handle, error) {
	return createVol(ctx, svc, volName, gotns.DefaultVolumeSpec())
}
