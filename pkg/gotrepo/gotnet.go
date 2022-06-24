package gotrepo

import (
	"context"
	"crypto/tls"
	"strings"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/mbapp"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/gotgrpc"
	"github.com/gotvc/got/pkg/gotnet"
	"github.com/gotvc/got/pkg/gotnet/quichub"
	"github.com/gotvc/got/pkg/goturl"
	"github.com/gotvc/got/pkg/logctx"
	"github.com/inet256/inet256/client/go_client/inet256client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Server serves cells, and blobs to the network.
func (r *Repo) Serve(ctx context.Context) error {
	srv, err := r.getGotNet()
	if err != nil {
		return err
	}
	u := goturl.NewNativeSpace(r.GetID())
	logctx.Infof(ctx, "serving at %s...", u.String())
	return srv.Serve(ctx)
}

func (r *Repo) ServeQUIC(ctx context.Context, laddr string) error {
	qh, err := quichub.Listen(r.privateKey, laddr)
	if err != nil {
		return err
	}
	gn := r.makeGotNet(qh)
	u := goturl.NewQUICSpace(quichub.Addr{ID: r.GetID(), Addr: laddr})
	logctx.Infof(ctx, "serving at %s ...", u.String())
	return gn.Serve(ctx)
}

func (r *Repo) GotNetClient() (*gotnet.Service, error) {
	return r.getGotNet()
}

func (r *Repo) getGotNet() (*gotnet.Service, error) {
	ctx := context.Background()
	if r.gotNet != nil {
		return r.gotNet, nil
	}
	client, err := inet256client.NewEnvClient()
	if err != nil {
		return nil, err
	}
	node, err := client.Open(ctx, r.privateKey)
	if err != nil {
		return nil, err
	}
	logctx.Infof(ctx, "setup INET256 node", node.LocalAddr())
	swarm := mbapp.New(inet256client.NewSwarm(node), gotnet.MaxMessageSize)
	srv := r.makeGotNet(swarm)
	r.gotNet = srv
	go r.gotNet.Serve(r.ctx)
	return srv, nil
}

func (r *Repo) getQUICGotNet(spec QUICSpaceSpec) (*gotnet.Service, error) {
	sw, err := quichub.Dial(r.privateKey, spec.ID, spec.Addr)
	if err != nil {
		return nil, err
	}
	gn := r.makeGotNet(sw)
	go gn.Serve(r.ctx)
	return gn, nil
}

func (r *Repo) makeGotNet(swarm p2p.SecureAskSwarm[PeerID]) *gotnet.Service {
	return gotnet.New(gotnet.Params{
		Open: func(peer PeerID) branches.Space {
			return r.iamEngine.GetSpace(r.GetSpace(), peer)
		},
		Swarm: swarm,
	})
}

func (r *Repo) getGRPCClient(endpoint string, headers map[string]string) (gotgrpc.GotSpaceClient, error) {
	ctx := context.Background()
	opts := []grpc.DialOption{}
	if strings.HasPrefix(endpoint, "http://") {
		endpoint = strings.TrimPrefix(endpoint, "http://")
		opts = append(opts, grpc.WithInsecure())
		logctx.Warnf(ctx, "insecure gRPC connection over http to %v", endpoint)
	} else if strings.HasPrefix(endpoint, "https://") {
		endpoint = strings.TrimPrefix(endpoint, "https://")
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	}
	if len(headers) > 0 {
		opts = append(opts, gotgrpc.WithHeaders(headers))
	}
	gc, err := grpc.DialContext(ctx, endpoint, opts...)
	if err != nil {
		return nil, err
	}
	return gotgrpc.NewGotSpaceClient(gc), nil
}
