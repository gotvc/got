package gotrepo

import (
	"context"
	"fmt"
	"maps"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotns"
	"github.com/gotvc/got/src/internal/gotcfg"
	"github.com/gotvc/got/src/internal/marks"
	"github.com/gotvc/got/src/internal/volumes"
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

func (r *Repo) CreateSpace(ctx context.Context, name string, spec SpaceSpec) error {
	if err := spec.Validate(); err != nil {
		return err
	}
	cfg, err := gotcfg.LoadFile[Config](r.root, configPath)
	if err != nil {
		return err
	}
	if _, exists := cfg.Spaces[name]; exists {
		return fmt.Errorf("a space with that name already exists")
	}
	return gotcfg.EditFile(r.root, configPath, func(x Config) Config {
		if _, exists := x.Spaces[name]; exists {
			return x
		}
		x.Spaces[name] = spec
		return x
	})
}

type VolumeSpec struct {
	URL    blobcache.URL `json:"url"`
	Secret gdat.DEK      `json:"secret"`
}

type SpaceSpec struct {
	// Blobcache is a arbitrary Blobcache Volume
	// The contents of the Volume are expected to be in the GotNS format.
	Blobcache *VolumeSpec `json:"bc,omitempty"`
}

func (ss SpaceSpec) Validate() error {
	var count int
	if ss.Blobcache != nil {
		count++
	}
	if count != 1 {
		return fmt.Errorf("spec must contain exactly 1 variant")
	}
	return nil
}

func (r *Repo) makeLocalSpace(ctx context.Context) (Space, error) {
	volh, secret, err := r.repoc.GetNamespace(ctx, r.config.RepoVolume, r.useSchema())
	if err != nil {
		return nil, err
	}
	return spaceFromHandle(r.bc, *volh, secret), nil
}

func (r *Repo) makeSpace(ctx context.Context, spec SpaceSpec) (Space, error) {
	switch {
	case spec.Blobcache != nil:
		bspec := *spec.Blobcache
		volh, err := bcsdk.OpenURL(ctx, r.bc, bspec.URL)
		if err != nil {
			return nil, err
		}
		return spaceFromHandle(r.bc, *volh, &bspec.Secret), nil
	default:
		return nil, fmt.Errorf("empty SpaceSpec")
	}
}

func spaceFromHandle(bc blobcache.Service, volh blobcache.Handle, secret *gdat.DEK) Space {
	var vol volumes.Volume = &volumes.Blobcache{
		Service: bc,
		Handle:  volh,
	}
	vol = volumes.NewChaCha20Poly1305(vol, (*[32]byte)(secret))
	kvmach := gotns.NewGotKV()
	return &gotns.Space{
		Volume: vol,
		KVMach: &kvmach,
		DMach:  gdat.NewMachine(),
	}
}
