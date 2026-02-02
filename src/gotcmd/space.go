package gotcmd

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/internal/gotcore"
	"go.brendoncarroll.net/star"
)

var spaceCmd = star.NewDir(star.Metadata{
	Short: "manage namespaces",
}, map[string]star.Command{
	"list":      spaceListCmd,
	"create-bc": spaceCreateBcCmd,
	"add-bc":    spaceAddBcCmd,
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
		c.Printf("%-30s %-19s %-19s\n", "NAME", "NODE", "OID")
		for name, scfg := range spaces {
			var oid blobcache.OID
			switch {
			case scfg.Blobcache != nil:
				oid = scfg.Blobcache.URL.Base
			default:
				c.Printf("ERROR: don't know how to print %v\n", scfg)
				continue
			}
			peerID := scfg.Blobcache.URL.Node
			c.Printf("%-30s %16s... %16s...\n", name, peerID.Base64String()[:16], oid.String()[:16])
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
	Flags: map[string]star.Flag{
		"add-prefix": addPrefixParam,
	},
	Pos: []star.Positional{srcSpaceParam, dstSpaceParam},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		var m func(string) string
		if prefix, ok := addPrefixParam.LoadOpt(c); ok {
			m = func(s string) string {
				return prefix + s
			}
		}
		task := gotrepo.SyncSpacesTask{
			Src:     srcSpaceParam.Load(c),
			Dst:     dstSpaceParam.Load(c),
			MapName: m,
		}
		return repo.SyncSpaces(ctx, task)
	},
}

var addPrefixParam = star.Optional[string]{
	ID:       "add-prefix",
	ShortDoc: "add a prefix to the destination names",
	Parse:    star.ParseString,
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

var spaceAddBcCmd = star.Command{
	Metadata: star.Metadata{
		Short: "adds an existing Space backed by a Blobcache Volume",
	},
	Pos: []star.Positional{spaceNameParam},
	Flags: map[string]star.Flag{
		"url":    bcURLParam,
		"secret": secretParam,
	},
	F: func(c star.Context) error {
		repo, err := openRepo()
		if err != nil {
			return err
		}
		bc := bcclient.NewClientFromEnv()
		u := bcURLParam.Load(c)
		volh, err := bcsdk.OpenURL(c, bc, u)
		if err != nil {
			return err
		}
		defer bc.Drop(c, *volh)
		_, err = bc.InspectVolume(c, *volh)
		if err != nil {
			return err
		}
		return repo.CreateSpace(c, spaceNameParam.Load(c), gotrepo.SpaceSpec{
			Blobcache: &gotrepo.VolumeSpec{
				URL:    u,
				Secret: secretParam.Load(c),
			},
		})
	},
}

var bcURLParam = star.Required[blobcache.URL]{
	ID: "bc-url",
	Parse: func(x string) (blobcache.URL, error) {
		u, err := blobcache.ParseURL(x)
		if err != nil {
			return blobcache.URL{}, err
		}
		return *u, nil
	},
}

var secretParam = star.Required[[32]byte]{
	ID: "secret",
	Parse: func(s string) ([32]byte, error) {
		data, err := hex.DecodeString(s)
		if err != nil {
			return [32]byte{}, nil
		}
		if len(data) != 32 {
			return [32]byte{}, fmt.Errorf("secret is wrong length")
		}
		return [32]byte(data), nil
	},
}

func randomSecret() (ret gdat.DEK) {
	rand.Read(ret[:])
	return ret
}

var spaceNameParam = star.Required[string]{
	ID: "space-name",
	Parse: func(x string) (string, error) {
		if err := gotcore.CheckName(x); err != nil {
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
