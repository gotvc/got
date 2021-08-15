package gotrepo

import (
	"context"

	"github.com/brendoncarroll/go-p2p/p/p2pmux"
	"github.com/gotvc/got/pkg/gotnet"
	"github.com/inet256/inet256/client/go_client/inet256client"
	"github.com/sirupsen/logrus"
)

// Server serves cells, and blobs to the network.
func (r *Repo) Serve(ctx context.Context) error {
	srv, err := r.getGotNet()
	if err != nil {
		return err
	}
	return srv.Serve()
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
	node, err := client.CreateNode(ctx, r.privateKey)
	if err != nil {
		return nil, err
	}
	swarm := inet256client.NewSwarm(node, r.privateKey.Public())
	mux := p2pmux.NewStringSecureAskMux(swarm)
	srv := gotnet.New(gotnet.Params{
		Logger: logrus.StandardLogger(),
		ACL:    r.GetACL(),
		Mux:    mux,
		Space:  r.GetSpace(),
	})
	r.gotNet = srv
	go r.gotNet.Serve()
	return srv, nil
}
