package gotcmd

import (
	"fmt"
	"os"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/blobcache"
	_ "blobcache.io/blobcache/src/blobcachecmd"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/gotwc"
	"github.com/gotvc/got/src/internal/gotcore"
	"go.brendoncarroll.net/star"
)

var initCmd = star.Command{
	Metadata: star.Metadata{
		Short: "initializes a repository in the current directory",
	},
	Flags: map[string]star.Flag{
		"blobcache": blobcacheParam,
		"mkvol":     mkvolParam,
		"volume":    volumeParam,
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
		if _, err := repo.CreateMark(c, gotrepo.FQM{Name: "master"}, gotcore.DefaultConfig(false), nil); err != nil {
			return err
		}
		// setup a working copy in the same directory
		if err := gotwc.Init(repo, root, gotwc.DefaultConfig()); err != nil {
			return err
		}
		c.Printf("successfully initialized got repo in current directory\n")
		return nil
	},
}

var mkvolParam = star.Optional[string]{
	ID:       "mkvol",
	Parse:    star.ParseString,
	ShortDoc: "the name to use when creating new a volume in a namespace",
}

var blobcacheParam = star.Optional[string]{
	ID:    "blobcache",
	Parse: star.ParseString,
}

func configureBlobcache(c star.Context, cfg *gotrepo.Config) error {
	configStr, _ := blobcacheParam.LoadOpt(c)
	switch configStr {
	case "env-client":
		volOID, volOk := volumeParam.LoadOpt(c)
		newVolName, mkvolOk := mkvolParam.LoadOpt(c)
		if mkvolOk == volOk {
			return fmt.Errorf("must provide one of --volume <oid> or --mkvol <name>")
		}
		if mkvolOk {
			bc := bcclient.NewClientFromEnv()
			volh, err := createRepoVol(c, bc, newVolName)
			if err != nil {
				return err
			}
			c.Printf("created new Blobcache Volume at %v\n", newVolName)
			volOID = volh.OID
		}
		apiVal := os.Getenv(bcclient.EnvBlobcacheAPI)
		c.Printf("using blobcache client at %v\n", apiVal)
		cfg.Blobcache = gotrepo.BlobcacheSpec{
			EnvClient: &gotrepo.EnvClientBlobcache{},
		}
		cfg.RepoVolume = volOID
		return nil
	case "", "in-process":
		cfg.Blobcache = gotrepo.BlobcacheSpec{
			InProcess: &gotrepo.InProcessBlobcache{},
		}
		cfg.RepoVolume = blobcache.OID{} // in-process puts repo at root OID
		return nil
	default:
		return fmt.Errorf("unrecognized blobcache option %v", configStr)
	}
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
