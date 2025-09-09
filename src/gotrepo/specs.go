package gotrepo

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/simplens"

	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/gotns"
	"github.com/gotvc/got/src/internal/volumes"
)

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
	nsc := simplens.Client{Service: r.bc}
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

func (r *Repo) MakeSpace(spec SpaceSpec) (Space, error) {
	switch {
	case spec.Local != nil:
		volh, err := r.bc.OpenAs(r.ctx, nil, *spec.Local, blobcache.Action_ALL)
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
					return r.MakeSpace(spec.Target)
				}),
			}
			layers = append(layers, layer)
		}
		return branches.NewMultiSpace(layers)
	case spec.Prefix != nil:
		inner, err := r.MakeSpace(spec.Prefix.Inner)
		if err != nil {
			return nil, err
		}
		return branches.NewPrefixSpace(inner, spec.Prefix.Prefix), nil
	default:
		return nil, fmt.Errorf("empty SpaceSpec")
	}
}
