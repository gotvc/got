package gotrepo

import (
	"context"
	"fmt"
	"slices"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotns"
	"github.com/gotvc/got/src/internal/volumes"
	"github.com/gotvc/got/src/marks"
)

// ListSpaces lists all the spaces that the repository is configured to use
func (r *Repo) ListSpaces(ctx context.Context) ([]SpaceConfig, error) {
	return slices.Clone(r.config.Spaces), nil
}

// GetSpace looks for a space with a matching name in the configuration
// if it finds a match, then the spec is used to construct a space
// and it is returned.
// If name is empty, then GetSpace returns the repos default namespace.
func (r *Repo) GetSpace(ctx context.Context, name string) (marks.Space, error) {
	if name == "" {
		return r.makeSpace(ctx, SpaceSpec{
			Local: &struct{}{},
		})
	}
	for _, scfg := range r.config.Spaces {
		if scfg.Name == "" {
			return nil, fmt.Errorf("spaces cannot have an empty name. fix .got/config")
		}
		if scfg.Name == name {
			return r.makeSpace(ctx, scfg.Spec)
		}
	}
	return nil, nil
}

type SpaceSpec struct {
	// Local is the namespace included in every repo.
	Local *struct{} `json:"local,omitempty"`
	// Blobcache is a arbitrary Blobcache Volume
	// The contents of the Volume are expected to be in the GotNS format.
	Blobcache *blobcache.VolumeSpec `json:"blobcache,omitempty"`
	// Org is an arbitrary Blobcache Volume
	// The contents of the Volume are expected to be in the GotOrg format
	Org *blobcache.VolumeSpec `json:"org,omitempty"`
}

func (r *Repo) makeSpace(ctx context.Context, spec SpaceSpec) (Space, error) {
	switch {
	case spec.Local != nil:
		volh, err := r.repoc.GetNamespace(ctx, r.config.RepoVolume, r.useSchema())
		if err != nil {
			return nil, err
		}
		return spaceFromHandle(r.bc, *volh), nil
	case spec.Blobcache != nil:
		vspec := *spec.Blobcache
		volh, err := r.bc.CreateVolume(ctx, nil, vspec)
		if err != nil {
			return nil, err
		}
		return spaceFromHandle(r.bc, *volh), nil
	default:
		return nil, fmt.Errorf("empty SpaceSpec")
	}
}

func spaceFromHandle(bc blobcache.Service, volh blobcache.Handle) Space {
	vol := &volumes.Blobcache{
		Service: bc,
		Handle:  volh,
	}
	kvmach := gotns.NewGotKV()
	return &gotns.Space{
		Volume: vol,
		KVMach: &kvmach,
		DMach:  gdat.NewMachine(),
	}
}
