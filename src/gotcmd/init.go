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
		"blobcache-http": blobcacheSysParam,
		"volume":         volumeParam,
	},
	F: func(c star.Context) error {
		config := gotrepo.DefaultConfig()
		// configure blobcache
		volume, volumeSet := volumeParam.LoadOpt(c)
		if volumeSet {
			config.RepoVolume = volume
		}
		bcCfg, err := specFromContext(c)
		if err != nil {
			return err
		}
		config.Blobcache = bcCfg
		if bcCfg.InProcess == nil && !volumeSet {
			return fmt.Errorf("must specify oid for out-of-process blobcache")
		}
		if err := gotrepo.Init(".", config); err != nil {
			return err
		}
		repo, err := gotrepo.Open(".")
		if err != nil {
			return err
		}
		defer repo.Close()
		if _, err := repo.CreateMark(c, gotrepo.FQM{Name: "master"}, marks.Params{}); err != nil {
			return err
		}
		if err := gotwc.Init(repo, ".", gotwc.Config{
			Head:  "master",
			ActAs: gotrepo.DefaultIden,
		}); err != nil {
			return err
		}
		c.Printf("successfully initialized got repo in current directory\n")
		return nil
	},
}

// specFromContext makes a Blobcache spec from a star.Context.
func specFromContext(c star.Context) (ret gotrepo.BlobcacheSpec, _ error) {
	blobcacheHttp, httpSet := blobcacheSysParam.LoadOpt(c)
	if httpSet {
		return gotrepo.BlobcacheSpec{}, fmt.Errorf("cannot specify both blobcache-remote and blobcache-http")
	}
	switch {
	case httpSet:
		c.Printf("using HTTP blobcache\n")
		ret.Client = &gotrepo.ExternalBlobcache{
			URL: blobcacheHttp,
		}
	default:
		c.Printf("using in-process blobcache\n")
		ret.InProcess = &gotrepo.InProcessBlobcache{
			ActAs: gotrepo.DefaultIden,
		}
	}
	return ret, nil
}

var blobcacheSysParam = star.Optional[string]{
	ID: "blobcache-sys",
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
		c.Printf("  |> got init --blobcache-http %s --volume %v\n", bcAPIStr, volh.OID)

		return nil
	},
}

var volNameParam = star.Required[string]{
	ID:       "vol-name",
	Parse:    star.ParseString,
	ShortDoc: "the name in the namespace to use for the new volume",
}
