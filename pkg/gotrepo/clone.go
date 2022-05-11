package gotrepo

import (
	"context"
	"fmt"

	"github.com/gotvc/got/pkg/gotnet/quichub"
	"github.com/gotvc/got/pkg/goturl"
	"github.com/inet256/inet256/pkg/inet256"
)

// Clone creates a new Repo at dirPath with origin mapping to the space at URL.
func Clone(ctx context.Context, u goturl.URL, dirPath string) error {
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

func spaceSpecFromURL(u goturl.URL) (*SpaceSpec, error) {
	switch u.Protocol {
	case goturl.ProtocolNative:
		id, err := inet256.ParseAddrB64([]byte(u.Host))
		if err != nil {
			return nil, err
		}
		return &SpaceSpec{
			Peer: &id,
		}, nil
	case goturl.ProtocolQUIC:
		addr, err := quichub.ParseAddr(u.Host)
		if err != nil {
			return nil, err
		}
		return &SpaceSpec{
			QUIC: &QUICSpaceSpec{
				Addr: addr.Addr,
				ID:   addr.ID,
			},
		}, nil
	case goturl.ProtocolGRPC:
		return &SpaceSpec{
			GRPC: &GRPCSpaceSpec{
				Endpoint: u.Host,
			},
		}, nil
	default:
		return nil, fmt.Errorf("unknown protocol %q", u.Protocol)
	}
}

func (r *Repo) GetNativeURL() goturl.URL {
	return goturl.NewNativeSpace(r.GetID())
}
