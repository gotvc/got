package gotcmd

import (
	"crypto/rand"
	"encoding/json"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/internal/marks"
	"go.brendoncarroll.net/star"
)

var spaceCmd = star.NewDir(star.Metadata{
	Short: "manage namespaces",
}, map[string]star.Command{
	"list":      spaceListCmd,
	"create-bc": spaceCreateBcCmd,
	"sync":      spaceSyncCmd,
})

var spaceListCmd = star.Command{
	Metadata: star.Metadata{Short: "list namespaces"},
	F: func(c star.Context) error {
		repo, err := openRepo()
		if err != nil {
			return err
		}
		spaces, err := repo.ListSpaces(c)
		if err != nil {
			return err
		}
		for name, scfg := range spaces {
			data, _ := json.Marshal(scfg)
			c.Printf("%s %s\n", name, data)
		}
		if len(spaces) == 0 {
			c.Printf("  (no spaces other than the default space)\n")
		}
		return nil
	},
}

var spaceSyncCmd = star.Command{
	Metadata: star.Metadata{
		Short: "copies marks from one space to another",
	},
	Pos: []star.Positional{srcSpaceParam, dstSpaceParam},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		task := gotrepo.SyncSpacesTask{
			Src: srcSpaceParam.Load(c),
			Dst: dstSpaceParam.Load(c),
		}
		return repo.SyncSpaces(ctx, task)
	},
}

var spaceCreateBcCmd = star.Command{
	Metadata: star.Metadata{
		Short: "create a new space backed by a Blobcache Volume",
	},
	Pos: []star.Positional{spaceNameParam},
	Flags: map[string]star.Flag{
		"mkvol": volNameParam,
	},
	F: func(c star.Context) error {
		repo, err := openRepo()
		if err != nil {
			return err
		}
		bcsvc := bcclient.NewClientFromEnv()
		ep, err := bcsvc.Endpoint(c)
		if err != nil {
			return err
		}
		volname := volNameParam.Load(c)
		h, err := createNSVol(c, bcsvc, volname)
		if err != nil {
			return err
		}
		return repo.CreateSpace(c, spaceNameParam.Load(c), gotrepo.SpaceSpec{
			Blobcache: &gotrepo.VolumeSpec{
				URL: blobcache.URL{
					Node: ep.Peer,
					Base: h.OID,
				},
				Secret: randomSecret(),
			},
		})
	},
}

func randomSecret() (ret gdat.DEK) {
	rand.Read(ret[:])
	return ret
}

var spaceNameParam = star.Required[string]{
	ID: "space-name",
	Parse: func(x string) (string, error) {
		if err := marks.CheckName(x); err != nil {
			return "", err
		}
		return x, nil
	},
}

var fetchCmd = star.Command{
	Metadata: star.Metadata{
		Short: "fetches marks from spaces according to the config",
	},
	Pos: []star.Positional{},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		if err := repo.Fetch(ctx); err != nil {
			return err
		}
		c.Printf("All fetch tasks completed successfully\n")
		return nil
	},
}

var pushCmd = star.Command{
	Metadata: star.Metadata{
		Short: "distributes marks to spaces according to the config",
	},
	Pos: []star.Positional{},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		if err := repo.Distribute(ctx); err != nil {
			return err
		}
		c.Printf("All distribute tasks completed successfully\n")
		return nil
	},
}

var srcSpaceParam = star.Required[string]{
	ID:    "src_space",
	Parse: star.ParseString,
}

var dstSpaceParam = star.Required[string]{
	ID:    "dst_space",
	Parse: star.ParseString,
}
