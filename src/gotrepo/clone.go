package gotrepo

import (
	"context"
	"os"
	"regexp"

	"blobcache.io/blobcache/src/blobcache"
)

// Clone creates a new Repo at dirPath with origin mapping to the space at URL.
func Clone(ctx context.Context, dirPath string, config Config, u blobcache.URL) error {
	if err := Init(dirPath, config); err != nil {
		return err
	}
	repoRoot, err := os.OpenRoot(dirPath)
	if err != nil {
		return err
	}
	defer repoRoot.Close()
	if err := EditConfig(repoRoot, func(x Config) Config {
		y := x
		y.Spaces["origin"] = SpaceSpec{Blobcache: &u}
		y.Fetch = []FetchConfig{
			{
				From:      "origin",
				Filter:    regexp.MustCompile(".*"),
				AddPrefix: "remote/origin/",
			},
		}
		return y
	}); err != nil {
		return err
	}
	return nil
}
