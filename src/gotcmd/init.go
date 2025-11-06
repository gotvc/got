package gotcmd

import (
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
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
		blobcacheRemote, remoteSet := blobcacheRemoteParam.LoadOpt(c)
		blobcacheHttp, httpSet := blobcacheHttpParam.LoadOpt(c)
		if remoteSet && httpSet {
			return fmt.Errorf("cannot specify both blobcache-remote and blobcache-http")
		}
		volume, volumeSet := volumeParam.LoadOpt(c)
		if volumeSet {
			config.RepoVolume = volume
		}

		config.Blobcache = gotrepo.BlobcacheSpec{}
		switch {
		case remoteSet:
			if !volumeSet {
				return fmt.Errorf("must provide --volume when using remote blobcache")
			}
			c.Printf("using remote blobcache\n")
			config.Blobcache.Remote = &blobcacheRemote
		case httpSet:
			if !volumeSet {
				return fmt.Errorf("must provide --volume when using remote blobcache")
			}
			c.Printf("using HTTP blobcache\n")
			config.Blobcache.HTTP = &blobcacheHttp
		default:
			c.Printf("using in-process blobcache\n")
			config.Blobcache.InProcess = &struct{}{}
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
