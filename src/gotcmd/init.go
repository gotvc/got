package gotcmd

import (
	"encoding/json"
	"fmt"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcachecmd"
	"github.com/gotvc/got/src/gotrepo"
	"go.brendoncarroll.net/star"
)

var initCmd = star.Command{
	Metadata: star.Metadata{
		Short: "initializes a repository in the current directory",
	},
	Flags: map[string]star.Flag{
		"blobcache-remote": blobcacheRemoteParam,
		"blobcache-http":   blobcacheHttpParam,
		"volume":           volumeParam,
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
		leaf, err := repo.ActiveIdentity(c.Context)
		if err != nil {
			return err
		}
		c.Printf("successfully initialized got repo in current directory\n")
		if config.Blobcache.Remote != nil {
			c.Printf("You must configure blobcache to allow access")
			c.Printf("PeerID: %v\n", leaf.ID)
			c.Printf("Repo Volume: %v\n", config.RepoVolume)
		}
		return nil
	},
}

// specFromContext makes a Blobcache spec from a star.Context.
func specFromContext(c star.Context) (ret gotrepo.BlobcacheSpec, _ error) {
	blobcacheRemote, remoteSet := blobcacheRemoteParam.LoadOpt(c)
	blobcacheHttp, httpSet := blobcacheHttpParam.LoadOpt(c)
	if remoteSet && httpSet {
		return gotrepo.BlobcacheSpec{}, fmt.Errorf("cannot specify both blobcache-remote and blobcache-http")
	}
	switch {
	case remoteSet:
		c.Printf("using remote blobcache\n")
		ret.Remote = &gotrepo.RemoteBlobcache{
			Endpoint: blobcacheRemote,
		}
	case httpSet:
		c.Printf("using HTTP blobcache\n")
		ret.HTTP = &gotrepo.HTTPBlobcache{
			URL: blobcacheHttp,
		}
	default:
		c.Printf("using in-process blobcache\n")
		ret.InProcess = &struct{}{}
	}
	return ret, nil
}

var blobcacheRemoteParam = star.Optional[blobcache.Endpoint]{
	ID: "blobcache-remote",
	Parse: func(s string) (blobcache.Endpoint, error) {
		return blobcache.ParseEndpoint(s)
	},
	ShortDoc: "the endpoint of the remote blobcache service",
}

var blobcacheHttpParam = star.Optional[string]{
	ID: "blobcache-http",
	Parse: func(s string) (string, error) {
		return s, nil
	},
	ShortDoc: "the URL of the HTTP blobcache service",
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
	})

var mkrepoCmd = star.Command{
	Metadata: star.Metadata{
		Short: "create a new Volume suitable for use as repo root",
	},
	Pos: []star.Positional{volNameParam},
	F: func(c star.Context) error {
		ctx := c.Context
		svc := bcclient.NewClientFromEnv()
		volName := volNameParam.Load(c)
		nsh := blobcache.Handle{} // assume the root
		nsc, err := blobcachecmd.NSClientForVolume(ctx, svc, nsh)
		if err != nil {
			return err
		}

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
		c.Printf("  |> If you are accessing blobcache over BCP then make sure the Node is configured to allow Got to access the volume\n")
		c.Printf("  |> Provide this volume in a call to got init like this:\n")
		c.Printf("  |> got init --volume %v\n", volh.OID)

		return nil
	},
}

var volNameParam = star.Required[string]{
	ID:       "vol-name",
	Parse:    star.ParseString,
	ShortDoc: "the name in the namespace to use for the new volume",
}
