package gotrepo

import (
	"context"
	"fmt"
	"maps"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotns"
	"github.com/gotvc/got/src/internal/volumes"
	"github.com/gotvc/got/src/marks"
)

// ListSpaces lists all the spaces that the repository is configured to use
func (r *Repo) ListSpaces(ctx context.Context) (map[string]SpaceSpec, error) {
	return maps.Clone(r.config.Spaces), nil
}

// GetSpace looks for a space with a matching name in the configuration
// if it finds a match, then the spec is used to construct a space
// and it is returned.
// If name is empty, then GetSpace returns the repos default namespace.
func (r *Repo) GetSpace(ctx context.Context, name string) (marks.Space, error) {
	if name == "" {
		return r.makeLocalSpace(ctx)
	}
	spec, ok := r.config.Spaces[name]
	if !ok {
		return nil, fmt.Errorf("space %q not found", name)
	}
	return r.makeSpace(ctx, spec)
}

type SpaceSpec struct {
	// Blobcache is a arbitrary Blobcache Volume
	// The contents of the Volume are expected to be in the GotNS format.
	Blobcache *blobcache.URL `json:"bc,omitempty"`
	// Org is an arbitrary Blobcache Volume
	// The contents of the Volume are expected to be in the GotOrg format
	Org *blobcache.URL `json:"org,omitempty"`
}

func (r *Repo) makeLocalSpace(ctx context.Context) (Space, error) {
	volh, err := r.repoc.GetNamespace(ctx, r.config.RepoVolume, r.useSchema())
	if err != nil {
		return nil, err
	}
	return spaceFromHandle(r.bc, *volh), nil
}

func (r *Repo) makeSpace(ctx context.Context, spec SpaceSpec) (Space, error) {
	switch {
	case spec.Blobcache != nil:
		volh, err := bcsdk.OpenURL(ctx, r.bc, *spec.Blobcache)
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
