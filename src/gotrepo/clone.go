package gotrepo

import (
	"context"
	"os"

	"blobcache.io/blobcache/src/blobcache"
)

// Clone creates a new Repo at dirPath with origin mapping to the space at URL.
func Clone(ctx context.Context, dirPath string, config Config, u blobcache.FQOID) error {
	if err := Init(dirPath, config); err != nil {
		return err
	}
	repoRoot, err := os.OpenRoot(dirPath)
	if err != nil {
		return err
	}
	defer repoRoot.Close()
	spaceSpec, err := spaceSpecFromURL(u)
	if err != nil {
		return err
	}
	if err := EditConfig(repoRoot, func(x Config) Config {
		y := x
		y.Spaces = []SpaceConfig{
			{
				Name: "origin/",
				Spec: *spaceSpec,
			},
		}
		// there shouldn't be anything here, but just in case, so we don't destroy anything.
		y.Spaces = append(y.Spaces, x.Spaces...)
		return y
	}); err != nil {
		return err
	}
	return nil
}

func spaceSpecFromURL(u blobcache.FQOID) (*SpaceSpec, error) {
	return &SpaceSpec{}, nil
}
