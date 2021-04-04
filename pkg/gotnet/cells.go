package gotnet

import (
	"context"
	"io"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/got/pkg/cells"
	"github.com/brendoncarroll/got/pkg/volumes"
)

type cellSrv struct {
	realm *volumes.Realm
	acl   ACL
	swarm p2p.AskSwarm
}

func newCellSrv(realm *volumes.Realm, acl ACL, swarm p2p.AskSwarm) *cellSrv {
	cs := &cellSrv{
		realm: realm,
		acl:   acl,
		swarm: swarm,
	}
	go p2p.ServeBoth(cs.swarm, p2p.NoOpTellHandler, cs.handleAsk)
	return cs
}

func (cs *cellSrv) CAS(ctx context.Context, dst p2p.PeerID, name string, prev, next []byte) ([]byte, error) {
	panic("not implemented")
}

func (cs *cellSrv) Get(ctx context.Context, dst p2p.PeerID, name string) ([]byte, error) {
	panic("not implemented")
}

func (cs *cellSrv) handleAsk(ctx context.Context, msg *p2p.Message, w io.Writer) {

}

var _ cells.Cell = &cell{}

type cell struct {
	srv *cellSrv
}

func (c *cell) CAS(ctx context.Context, prev, next []byte) (bool, []byte, error) {
	panic("not implemented")
}

func (c *cell) Get(ctx context.Context) ([]byte, error) {
	panic("not implemented")
}
