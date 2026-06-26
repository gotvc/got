package gotrepo

import (
	"context"
	"fmt"
	"maps"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotns"
	"github.com/gotvc/got/src/internal/gotcore"
	"github.com/gotvc/got/src/internal/volumes"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

// ListSpaces lists all the spaces that the repository is configured to use
func (r *Repo) ListSpaces(ctx context.Context) (map[string]SpaceSpec, error) {
	return maps.Clone(r.config.Spaces), nil
}

// GetSpace looks for a space with a matching name in the configuration
// if it finds a match, then the spec is used to construct a space
// and it is returned.
// If name is empty, then GetSpace returns the repos default namespace.
func (r *Repo) GetSpace(ctx context.Context, name string) (gotcore.Space, error) {
	if name == "" {
		return r.makeLocalSpace(ctx)
	}
	spec, ok := r.config.Spaces[name]
	if !ok {
		return nil, fmt.Errorf("space %q not found", name)
	}
	return r.makeSpace(ctx, spec)
}

func (r *Repo) AddSpace(ctx context.Context, name string, spec SpaceSpec) error {
	if err := spec.Validate(); err != nil {
		return err
	}
	var success bool
	if err := r.Configure(ctx, func(x Config) (Config, error) {
		if _, exists := x.Spaces[name]; exists {
			return x, nil
		}

		x.Spaces[name] = spec
		success = true
		return x, nil
	}); err != nil {
		return err
	}
	if !success {
		return fmt.Errorf("a space with that name already exists")
	}
	return nil
}

func (r *Repo) RemoveSpace(ctx context.Context, name string) error {
	return r.Configure(ctx, func(x Config) (Config, error) {
		delete(x.Spaces, name)
		return x, nil
	})
}

// RenameSpace moves the space configured at oldName, to newName.
// if there is already a Space at newName, then an error is returned.
// It also renames references in Pull and Push configurations.
func (r *Repo) RenameSpace(ctx context.Context, oldName, newName string) error {
	return r.Configure(ctx, func(x Config) (Config, error) {
		spec, exists := x.Spaces[oldName]
		if !exists {
			return x, fmt.Errorf("%s does not exist", oldName)
		}
		if _, exists := x.Spaces[newName]; exists {
			return x, fmt.Errorf("%s already exists", newName)
		}
		delete(x.Spaces, oldName)
		x.Spaces[newName] = spec
		for i := range x.Pull {
			if x.Pull[i].From == oldName {
				x.Pull[i].From = newName
			}
		}
		for i := range x.Push {
			if x.Push[i].To == oldName {
				x.Push[i].To = newName
			}
		}
		return x, nil
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
	volh, secret, err := r.repoc.GetNamespace(ctx, r.rootVol, false)
	if err != nil {
		return nil, err
	}
	return spaceFromHandle(r.bc, *volh, secret), nil
}

func (r *Repo) makeSpace(ctx context.Context, spec SpaceSpec) (Space, error) {
	switch {
	case spec.Blobcache != nil:
		bspec := *spec.Blobcache
		logctx.Debug(ctx, "opening url", zap.Stringer("url", bspec.URL))
		volh, err := bcsdk.OpenURL(ctx, r.bc, bspec.URL)
		if err != nil {
			return nil, err
		}
		logctx.Debug(ctx, "opened volume for URL", zap.Stringer("url", bspec.URL))
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
		DMach:  gdat.NewMachine(gdat.Params{}),
	}
}
