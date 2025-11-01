package gotrepo

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/basicns"

	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/gotns"
	"github.com/gotvc/got/src/internal/volumes"
)

// BlobcacheSpec describes how to access a Blobcache Service.
type BlobcacheSpec struct {
	// InProcess uses an in-process Blobcache service.
	// The state will be stored in the .got/blobcache directory.
	// This is the default.
	// The state can get quite large for large datasets, so it is recommended to use the system's Blobcache.
	InProcess *struct{} `json:"in_process,omitempty"`
	// HTTP uses an HTTP Blobcache service.
	// This is plaintext, non-encrypted HTTP, and it does not require authentication.
	// This should only be used for connecting on local host or via a unix socket.
	HTTP *string `json:"http,omitempty"`
	// Remote uses the Blobcache Protocol (BCP), and Got will appear as a Blobcache Node to the service.
	// This is a binary protocol and has less overhead than HTTP.
	// Got will not serve any requests that it receieves while acting as a dummy Node.
	Remote *blobcache.Endpoint `json:"remote,omitempty"`
}

// VolumeSpec explains how to create a Volume in blobcache.
type VolumeSpec = blobcache.VolumeSpec

func ParseVolumeSpec(data []byte) (*VolumeSpec, error) {
	var spec VolumeSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, err
	}
	return &spec, nil
}

func (r *Repo) MakeVolume(ctx context.Context, branchName string, spec VolumeSpec) (branches.Volume, error) {
	nsc := basicns.Client{Service: r.bc}
	volh, err := nsc.OpenAt(ctx, blobcache.Handle{}, branchName, blobcache.Action_ALL)
	if err != nil {
		if strings.Contains(err.Error(), "entry not found") {
			volh, err = nsc.CreateAt(ctx, blobcache.Handle{}, branchName, spec)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	return &volumes.Blobcache{Service: r.bc, Handle: *volh}, nil
}

// MultiSpaceSpec is a prefix-routed space made up of layers.
type MultiSpaceSpec []SpaceLayerSpec

// SpaceLayerSpec is a layer in a multi-layer space.
type SpaceLayerSpec struct {
	Prefix string    `json:"prefix"`
	Target SpaceSpec `json:"target"`
}

type PrefixSpaceSpec struct {
	Inner  SpaceSpec `json:"inner"`
	Prefix string    `json:"prefix"`
}

type SpaceSpec struct {
	Local     *blobcache.OID `json:"local,omitempty"`
	Blobcache *VolumeSpec    `json:"blobcache,omitempty"`

	Multi  *MultiSpaceSpec  `json:"multi,omitempty"`
	Prefix *PrefixSpaceSpec `json:"prefix,omitempty"`
}

func (r *Repo) MakeSpace(ctx context.Context, spec SpaceSpec) (Space, error) {
	switch {
	case spec.Local != nil:
		volh, err := r.bc.OpenFiat(ctx, *spec.Local, blobcache.Action_ALL)
		if err != nil {
			return nil, err
		}
		return gotns.NewSpace(&r.gnsc, *volh), nil
	case spec.Blobcache != nil:
		return nil, fmt.Errorf("blobcache spaces in arbitrary volumes are not yet supported")
	case spec.Multi != nil:
		var layers []branches.Layer
		for _, spec := range *spec.Multi {
			layer := branches.Layer{
				Prefix: spec.Prefix,
				Target: newLazySpace(func(ctx context.Context) (branches.Space, error) {
					return r.MakeSpace(ctx, spec.Target)
				}),
			}
			layers = append(layers, layer)
		}
		return branches.NewMultiSpace(layers)
	case spec.Prefix != nil:
		inner, err := r.MakeSpace(ctx, spec.Prefix.Inner)
		if err != nil {
			return nil, err
		}
		return branches.NewPrefixSpace(inner, spec.Prefix.Prefix), nil
	default:
		return nil, fmt.Errorf("empty SpaceSpec")
	}
}
