package gotcmd

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"

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
		repo, close, err := openRepo()
		if err != nil {
			return err
		}
		defer close()
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
		repo, close, err := openRepo()
		if err != nil {
			return err
		}
		defer close()
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
		_, err = repo.SyncSpaces(ctx, task)
		return err
	},
}

var addPrefixParam = &star.Optional[string]{
	PosName:  "add-prefix",
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
		repo, close, err := openRepo()
		if err != nil {
			return err
		}
		defer close()
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
					Node: ep.Node,
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
		repo, close, err := openRepo()
		if err != nil {
			return err
		}
		defer close()
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

var bcURLParam = &star.Required[blobcache.URL]{
	PosName: "bc-url",
	Parse: func(x string) (blobcache.URL, error) {
		u, err := blobcache.ParseURL(x)
		if err != nil {
			return blobcache.URL{}, err
		}
		return *u, nil
	},
}

var secretParam = &star.Required[[32]byte]{
	PosName: "secret",
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

var spaceNameParam = &star.Required[string]{
	PosName: "space-name",
	Parse: func(x string) (string, error) {
		if err := gotcore.CheckName(x); err != nil {
			return "", err
		}
		return x, nil
	},
}

var pullCmd = star.Command{
	Metadata: star.Metadata{
		Short: "pulls marks from spaces according to the config",
	},
	Pos: []star.Positional{},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, close, err := openRepo()
		if err != nil {
			return err
		}
		defer close()
		var hadErrors bool
		if err := repo.Pull(ctx, func(sr gotrepo.SyncResult) {
			for _, item := range sr.Items {
				if item.Err != nil {
					hadErrors = true
				}
			}
			printSyncResult(&c, sr)
		}); err != nil {
			return err
		}
		if !hadErrors {
			c.Printf("pull completed successfully\n")
		} else {
			c.Printf("pull completed with partial success.")
		}
		return nil
	},
}

var pushCmd = star.Command{
	Metadata: star.Metadata{
		Short: "pushes marks to spaces according to the config",
	},
	Pos: []star.Positional{},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, close, err := openRepo()
		if err != nil {
			return err
		}
		defer close()
		var hadErrors bool
		if err := repo.Push(ctx, func(sr gotrepo.SyncResult) {
			for _, item := range sr.Items {
				if item.Err != nil {
					hadErrors = true
				}
			}
			printSyncResult(&c, sr)
		}); err != nil {
			return err
		}
		if !hadErrors {
			c.Printf("push completed successfully\n")
		} else {
			c.Printf("push completed with partial success.")
		}
		return nil
	},
}

func printSyncResult(c *star.Context, sr gotrepo.SyncResult) error {
	slices.SortFunc(sr.Items, func(a, b gotcore.SyncResult) int {
		return strings.Compare(a.Dst, b.Dst)
	})
	c.Printf("%s -> %s\n", sr.Src, sr.Dst)
	const fmtStr = "%-30s -> %-30s %s\n"
	for _, res := range sr.Items {
		switch {
		case res.WasDeleted():
			c.Printf(fmtStr, res.Dst, "(deleted)")
		case res.WasCreated():
			c.Printf(fmtStr, res.Src, res.Dst, "(created)")
		case res.WasUpdated():
			c.Printf(fmtStr, res.Src, res.Dst, "(updated)")
		default:
			// c.Printf("%-30s -> %s (unchanged)\n", res.Src, res.Dst)
		}
	}
	c.Printf("\n")
	return nil
}

var srcSpaceParam = &star.Required[string]{
	PosName: "src_space",
	Parse:   star.ParseString,
}

var dstSpaceParam = &star.Required[string]{
	PosName: "dst_space",
	Parse:   star.ParseString,
}
