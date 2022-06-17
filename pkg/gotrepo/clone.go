package gotrepo

import (
	"context"
	"fmt"

	"github.com/gotvc/got/pkg/gotnet/quichub"
	"github.com/gotvc/got/pkg/goturl"
	"github.com/inet256/inet256/pkg/inet256"
)

// Clone creates a new Repo at dirPath with origin mapping to the space at URL.
func Clone(ctx context.Context, u goturl.SpaceURL, dirPath string) error {
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

func spaceSpecFromURL(u goturl.SpaceURL) (*SpaceSpec, error) {
	var spec *SpaceSpec
	switch u.Protocol {
	case goturl.ProtocolNative:
		id, err := inet256.ParseAddrBase64([]byte(u.Host))
		if err != nil {
			return nil, err
		}
		spec = &SpaceSpec{
			Peer: &id,
		}
	case goturl.ProtocolQUIC:
		addr, err := quichub.ParseAddr(u.Host)
		if err != nil {
			return nil, err
		}
		spec = &SpaceSpec{
			QUIC: &QUICSpaceSpec{
				Addr: addr.Addr,
				ID:   addr.ID,
			},
		}
	case goturl.ProtocolGRPC:
		spec = &SpaceSpec{
			GRPC: &GRPCSpaceSpec{
				Endpoint: u.Host,
			},
		}
	default:
		return nil, fmt.Errorf("unknown protocol %q", u.Protocol)
	}
	if u.SpacePrefix != "" {
		spec = &SpaceSpec{
			Prefix: &PrefixSpaceSpec{
				Prefix: u.SpacePrefix,
				Inner:  *spec,
			},
		}
	}
	return spec, nil
}

func (r *Repo) GetNativeURL() goturl.SpaceURL {
	return goturl.NewNativeSpace(r.GetID())
}
