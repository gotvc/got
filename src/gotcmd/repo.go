package gotcmd

import (
	"context"
	"encoding/json"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/blobcache"
	_ "blobcache.io/blobcache/src/blobcachecmd"
	"blobcache.io/blobcache/src/schema/bcns"
	"github.com/gotvc/got/src/gotns"
	"github.com/gotvc/got/src/gotrepo"
	"go.brendoncarroll.net/star"
)

var repoCmd = star.NewDir(star.Metadata{
	Short: "repo maintenance commands",
}, map[string]star.Command{
	"repair-links": repairLinksCmd,
	"init":         repoInitCmd,
	"create":       repoCreateCmd,
	"add-push":     configAddPushCmd,
	"add-pull":     configAddPullCmd,
	"rm-push":      configRmPushCmd,
	"rm-pull":      configRmPullCmd,
})

var repoInitCmd = star.Command{
	Metadata: star.Metadata{
		Short: "initialize a Repo in an existing Blobcache Volume.  Does not create a working copy.",
	},
	Pos: []star.Positional{volNameParam},
	F: func(c star.Context) error {
		ctx := c.Context
		bc := bcclient.NewClientFromEnv()
		nsVol, nsc, err := bcclient.OpenNSRoot(ctx, bc)
		if err != nil {
			return err
		}
		volh, err := bcns.Lookup(ctx, nsc, *nsVol, volNameParam.Load(c))
		if err != nil {
			return err
		}
		if err := gotrepo.Init(ctx, bc, *volh, gotrepo.DefaultConfig()); err != nil {
			return err
		}
		c.Printf("repo initialized successfully")
		return nil
	},
}

var repoCreateCmd = star.Command{
	Metadata: star.Metadata{
		Short: "create a new Volume and initialize a Repo in it.",
	},
	Pos: []star.Positional{volNameParam},
	F: func(c star.Context) error {
		ctx := c.Context
		svc := bcclient.NewClientFromEnv()
		ep, err := svc.Endpoint(ctx)
		if err != nil {
			return err
		}
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
		c.Printf("NODE: %v", ep.Node)
		c.Printf("HANDLE: %v\n", *volh)
		c.Printf("INFO: %s\n", prettifyJSON(specJSON))

		c.Printf("initializing volume...\n")
		if err := gotrepo.Init(ctx, svc, *volh, gotrepo.DefaultConfig()); err != nil {
			return err
		}
		c.Printf("successfully initialized Repo in %v", volh.OID)
		return nil
	},
}

var volNameParam = &star.Required[string]{
	PosName:  "vol-name",
	Parse:    star.ParseString,
	ShortDoc: "the name in the namespace to use for the new volume",
}

var scrubCmd = star.Command{
	Metadata: star.Metadata{
		Short: "runs integrity checks",
	},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, close, err := openRepo()
		if err != nil {
			return err
		}
		defer close()
		if err := repo.CheckAll(ctx); err != nil {
			return err
		}
		c.Printf("everything is OK\n")
		return nil
	},
}

var repairLinksCmd = star.Command{
	Metadata: star.Metadata{
		Short: "repairs repo volume link tokens",
	},
	F: func(c star.Context) error {
		repo, close, err := openRepo()
		if err != nil {
			return err
		}
		defer close()
		if err := gotrepo.RepairRepoLinks(c.Context, repo); err != nil {
			return err
		}
		c.Printf("repaired repo links\n")
		return nil
	},
}

var blobcacheCmd = star.NewDir(
	star.Metadata{Short: "manage blobcache"},
	map[string]star.Command{},
)

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
