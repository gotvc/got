package gotcmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/blobcache"
	_ "blobcache.io/blobcache/src/blobcachecmd"
	"blobcache.io/blobcache/src/schema/bcns"
	"github.com/gotvc/got/src/gotns"
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
		if _, err := repo.CreateMark(c, gotrepo.FQM{Name: "master"}, marks.DefaultConfig(false), nil); err != nil {
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

var blobcacheCmd = star.NewDir(
	star.Metadata{Short: "manage blobcache"},
	map[string]star.Command{
		"mkrepo": mkrepoCmd,
	},
)

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

func createRepoVol(ctx context.Context, svc blobcache.Service, volName string) (*blobcache.Handle, error) {
	nsc, err := bcns.ClientForVolume(ctx, svc, bcns.Objectish{})
	if err != nil {
		return nil, err
	}
	nsh := blobcache.Handle{} // assume the root
	spec := gotrepo.RepoVolumeSpec(false)
	return nsc.CreateAt(ctx, nsh, volName, spec)
}

func createNSVol(ctx context.Context, svc blobcache.Service, volName string) (*blobcache.Handle, error) {
	nsc, err := bcns.ClientForVolume(ctx, svc, bcns.Objectish{})
	if err != nil {
		return nil, err
	}
	nsh := blobcache.Handle{} // assume the root
	spec := gotns.DefaultVolumeSpec()
	return nsc.CreateAt(ctx, nsh, volName, spec)
}
