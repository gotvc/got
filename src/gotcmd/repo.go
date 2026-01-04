package gotcmd

import (
	"encoding/json"
	"fmt"
	"os"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/bcns"
	"blobcache.io/blobcache/src/schema/jsonns"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/gotwc"
	"github.com/gotvc/got/src/internal/marks"
	"go.brendoncarroll.net/star"
)

var initCmd = star.Command{
	Metadata: star.Metadata{
		Short: "initializes a repository in the current directory",
	},
	Flags: map[string]star.Flag{
		"blobcache-client": blobcacheClientParam,
		"volume":           volumeParam,
	},
	F: func(c star.Context) error {
		config := gotrepo.DefaultConfig()
		volume, volumeSet := volumeParam.LoadOpt(c)
		if volumeSet {
			config.RepoVolume = volume
		}
		// configure blobcache
		if err := configureBlobcache(c, &config); err != nil {
			return err
		}
		root, err := os.OpenRoot(".")
		if err != nil {
			return err
		}
		defer root.Close()
		// setup repo
		if err := gotrepo.Init(root, config); err != nil {
			return err
		}
		repo, err := gotrepo.Open(root)
		if err != nil {
			return err
		}
		defer repo.Close()
		// create the first branch
		if _, err := repo.CreateMark(c, gotrepo.FQM{Name: "master"}, marks.Metadata{}); err != nil {
			return err
		}
		// setup a working copy in the same directory
		if err := gotwc.Init(repo, root, gotwc.Config{
			Head:  "master",
			ActAs: gotrepo.DefaultIden,
		}); err != nil {
			return err
		}
		c.Printf("successfully initialized got repo in current directory\n")
		return nil
	},
}

func configureBlobcache(c star.Context, cfg *gotrepo.Config) error {
	blobcacheAPI, clientOK := blobcacheClientParam.LoadOpt(c)
	var bcspec gotrepo.BlobcacheSpec
	switch {
	case clientOK:
		if _, hasVol := volumeParam.LoadOpt(c); !hasVol {
			return fmt.Errorf("must provide volume when using --blobcache-client")
		}
		c.Printf("using blobcache client at %v\n", blobcacheAPI)
		bcspec.Client = &gotrepo.ExternalBlobcache{
			URL: blobcacheAPI,
		}
	default:
		c.Printf("using in-process blobcache\n")
		bcspec.InProcess = &gotrepo.InProcessBlobcache{
			ActAs: gotrepo.DefaultIden,
		}
	}
	cfg.Blobcache = bcspec
	return nil
}

var blobcacheClientParam = star.Optional[string]{
	ID: "blobcache-client",
	Parse: func(s string) (string, error) {
		return s, nil
	},
	ShortDoc: "the URL of the system Blobcache service (this is usually a unix socket)",
}

var volumeParam = star.Optional[blobcache.OID]{
	ID: "volume",
	Parse: func(s string) (blobcache.OID, error) {
		return blobcache.ParseOID(s)
	},
	ShortDoc: "the OID of the volume to use for the repo",
}

var scrubCmd = star.Command{
	Metadata: star.Metadata{
		Short: "runs integrity checks",
	},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		if err := repo.CheckAll(ctx); err != nil {
			return err
		}
		c.Printf("everything is OK\n")
		return nil
	},
}

var blobcacheCmd = star.NewDir(
	star.Metadata{Short: "manage blobcache"},
	map[string]star.Command{
		"mkrepo": mkrepoCmd,
	},
)

var mkrepoCmd = star.Command{
	Metadata: star.Metadata{
		Short: "create a new Volume suitable for use as repo root",
	},
	Pos: []star.Positional{volNameParam},
	F: func(c star.Context) error {
		ctx := c.Context
		svc := bcclient.NewClientFromEnv()
		bcAPIStr := os.Getenv(bcclient.EnvBlobcacheAPI)
		volName := volNameParam.Load(c)
		nsc := bcns.Client{
			Service: svc,
			Schema:  jsonns.Schema{},
		}
		nsh := blobcache.Handle{} // assume the root

		spec := gotrepo.RepoVolumeSpec(false)
		volh, err := nsc.CreateAt(ctx, nsh, volName, spec)
		if err != nil {
			return err
		}
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
		c.Printf("  |> got init --blobcache-client %s --volume %v\n", bcAPIStr, volh.OID)

		return nil
	},
}

var volNameParam = star.Required[string]{
	ID:       "vol-name",
	Parse:    star.ParseString,
	ShortDoc: "the name in the namespace to use for the new volume",
}
