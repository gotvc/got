package got

import (
	"context"

	"github.com/brendoncarroll/go-p2p/p/stringmux"
	"github.com/brendoncarroll/got/pkg/gotnet"
	"github.com/inet256/inet256/pkg/inet256p2p"
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

func (r *Repo) getGotNet() (*gotnet.Service, error) {
	if r.gotNet != nil {
		return r.gotNet, nil
	}
	swarm, err := inet256p2p.NewSwarm("127.0.0.1:25600", r.privateKey)
	if err != nil {
		return nil, err
	}
	mux := stringmux.WrapSecureAskSwarm(swarm)
	srv := gotnet.New(gotnet.Params{
		Logger: logrus.New(),
		ACL:    r.GetACL(),
		Mux:    mux,
		Realm:  r.GetRealm(),
	})
	r.gotNet = srv
	return srv, nil
}
