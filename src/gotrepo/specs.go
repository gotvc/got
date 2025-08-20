package gotrepo

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/simplens"

	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/internal/volumes"
)

type VolumeSpec = blobcache.VolumeSpec

type BranchSpec struct {
	Volume blobcache.VolumeSpec `json:"volume"`
	branches.Info
}

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

type MultiSpaceSpec []SpaceLayerSpec

type SpaceLayerSpec struct {
	Prefix string    `json:"prefix"`
	Target SpaceSpec `json:"target"`
}

type GRPCSpaceSpec struct {
	Endpoint string            `json:"endpoint"`
	Headers  map[string]string `json:"headers,omitempty"`
}

type CryptoSpaceSpec struct {
	Inner       SpaceSpec `json:"inner"`
	Secret      []byte    `json:"secret"`
	Passthrough []string  `json:"passthrough,omitempty"`
}

type PrefixSpaceSpec struct {
	Inner  SpaceSpec `json:"inner"`
	Prefix string    `json:"prefix"`
}

type SpaceSpec struct {
	GRPC *GRPCSpaceSpec `json:"grpc,omitempty"`
	// Mount is a Space encoded in a single volume.
	Mount *VolumeSpec `json:"mount,omitempty"`

	Crypto *CryptoSpaceSpec `json:"crypto,omitempty"`
	Prefix *PrefixSpaceSpec `json:"prefix,omitempty"`
}

func (r *Repo) MakeSpace(spec SpaceSpec) (Space, error) {
	switch {
	case spec.Crypto != nil:
		secret := spec.Crypto.Secret
		if len(secret) != branches.SecretSize {
			return nil, fmt.Errorf("crypto secret key is wrong size. HAVE: %d WANT: %d", len(secret), branches.SecretSize)
		}
		innerSpec := spec.Crypto.Inner
		inner, err := r.MakeSpace(innerSpec)
		if err != nil {
			return nil, err
		}
		var opts []branches.CryptoSpaceOption
		if spec.Crypto.Passthrough != nil {
			opts = append(opts, branches.WithPassthrough(spec.Crypto.Passthrough))
		}
		return branches.NewCryptoSpace(inner, (*[32]byte)(secret), opts...), nil
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

func (r *Repo) spaceFromSpecs(specs []SpaceLayerSpec) (branches.Space, error) {
	var layers []branches.Layer
	for _, spec := range specs {
		layer := branches.Layer{
			Prefix: spec.Prefix,
			Target: newLazySpace(func(ctx context.Context) (branches.Space, error) {
				return r.MakeSpace(spec.Target)
			}),
		}
		layers = append(layers, layer)
	}
	layers = append(layers, branches.Layer{
		Prefix: "",
		Target: r.space,
	})
	return branches.NewMultiSpace(layers)
}
