package gotcmd

import (
	"fmt"
	"os"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/gotwc"
	"github.com/gotvc/got/src/internal/gotbc"
	"github.com/gotvc/got/src/internal/gotcore"
	"go.brendoncarroll.net/star"
)

var initCmd = star.Command{
	Metadata: star.Metadata{
		Short: "initializes repository and working copy in the current directory",
	},
	Flags: map[string]star.Flag{
		"blobcache": blobcacheParam,
		"mkvol":     mkvolParam,
		"volume":    volumeParam,
	},
	F: func(c star.Context) error {
		config := gotwc.DefaultConfig()
		volume, volumeSet := volumeParam.LoadOpt(c)
		if volumeSet {
			config.Repo = volume
		}
		// configure blobcache
		if err := configureBlobcache(c, &config); err != nil {
			return err
		}
		ctx := c.Context
		root, err := os.OpenRoot(".")
		if err != nil {
			return err
		}
		defer root.Close()
		// setup blobcache, create repo
		if err := func() error {
			bcsvc, err := gotbc.OpenBlobcache(root, config.Blobcache, ctx)
			if err != nil {
				return err
			}
			if bclocal, ok := bcsvc.(*gotbc.Local); ok {
				c.Printf("using internal blobcache\n")
				defer bclocal.Close()
			}
			repoVolh, err := bcsvc.OpenFiat(ctx, config.Repo, blobcache.Action_ALL)
			if err != nil {
				return err
			}
			repoCfg := gotrepo.DefaultConfig()
			if err := gotrepo.Init(ctx, bcsvc, *repoVolh, repoCfg); err != nil {
				return err
			}
			c.Printf("successfully initialized repo in %v\n", config.Repo)
			repo, err := gotrepo.Open(ctx, bcsvc, config.Repo, nil)
			if err != nil {
				return err
			}
			dsConfig := gotcore.DefaultConfig(false)
			_, err = repo.CreateMark(ctx, gotrepo.DefaultMark(), dsConfig, nil)
			return err
		}(); err != nil {
			return err
		}

		// setup a working copy in the same directory
		if err := gotwc.Init(root, gotwc.DefaultConfig()); err != nil {
			return err
		}
		c.Printf("successfully initialized working copy in current directory\n")
		return nil
	},
}

var mkvolParam = &star.Optional[string]{
	PosName:  "mkvol",
	Parse:    star.ParseString,
	ShortDoc: "the name to use when creating new a volume in a namespace",
}

var blobcacheParam = &star.Optional[string]{
	PosName:  "blobcache",
	Parse:    star.ParseString,
	ShortDoc: "configure how to use Blobcache",
}

// configureBlobcache reads parameters from c
// and creates Volumes in blobcache and writes config changes to cfg.
func configureBlobcache(c star.Context, cfg *gotwc.Config) error {
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
		cfg.Blobcache = gotbc.Config{
			EnvClient: &gotbc.EnvClientSpec{},
		}
		cfg.Repo = volOID
		return nil
	case "", "in-process":
		cfg.Blobcache = gotwc.BlobcacheSpec{
			InProcess: &gotbc.InProcessSpec{},
		}
		cfg.Repo = blobcache.OID{} // in-process puts repo at root OID
		return nil
	default:
		return fmt.Errorf("unrecognized blobcache option %v", configStr)
	}
}

var volumeParam = &star.Optional[blobcache.OID]{
	PosName: "volume",
	Parse: func(s string) (blobcache.OID, error) {
		return blobcache.ParseOID(s)
	},
	ShortDoc: "the OID of the volume to use for the repo",
}
