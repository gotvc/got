package gotrepo

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
)

// Clone creates a new Repo at dirPath with origin mapping to the space at URL.
func Clone(ctx context.Context, u blobcache.FQOID, dirPath string) error {
	if err := Init(dirPath); err != nil {
		return err
	}
	spaceSpec, err := spaceSpecFromURL(u)
	if err != nil {
		return err
	}
	if err := ConfigureRepo(dirPath, func(x Config) Config {
		y := x
		y.Spaces = []SpaceLayerSpec{
			{
				Prefix: "origin/",
				Target: *spaceSpec,
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
