package gotnet

import (
	"context"
	"testing"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/mbapp"
	"github.com/brendoncarroll/go-p2p/p2ptest"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/cadata/storetest"
	"github.com/brendoncarroll/go-state/cells/celltest"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/cells"
	"github.com/inet256/inet256/client/go_client/inet256client"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/stretchr/testify/require"
)

func TestSpace(t *testing.T) {
	branches.TestSpace(t, func(t testing.TB) branches.Space {
		s1, s2 := newTestPair(t)
		go s1.srv.Serve()
		go s2.srv.Serve()
		peer2 := s2.swarm.LocalAddrs()[0]
		return s1.srv.GetSpace(peer2)
	})
}

func TestStore(t *testing.T) {
	storetest.TestStore(t, func(t testing.TB) cadata.Store {
		s1, s2 := newTestPair(t)
		go s1.srv.Serve()
		go s2.srv.Serve()
		peer2 := s2.swarm.LocalAddrs()[0]
		space := s1.srv.GetSpace(peer2)
		return createStore(t, space)
	})
}

func TestCell(t *testing.T) {
	celltest.CellTestSuite(t, func(t testing.TB) cells.Cell {
		s1, s2 := newTestPair(t)
		go s1.srv.Serve()
		go s2.srv.Serve()
		peer2 := s2.swarm.LocalAddrs()[0]
		space := s1.srv.GetSpace(peer2)
		return createCell(t, space)
	})
}

func createCell(t testing.TB, space branches.Space) cells.Cell {
	name := "test"
	ctx := context.Background()
	_, err := space.Create(ctx, name, branches.Params{})
	require.NoError(t, err)
	branch, err := space.Get(ctx, name)
	require.NoError(t, err)
	return branch.Volume.Cell
}

func createStore(t testing.TB, space branches.Space) cadata.Store {
	name := "test"
	ctx := context.Background()
	_, err := space.Create(ctx, name, branches.Params{})
	require.NoError(t, err)
	branch, err := space.Get(ctx, name)
	require.NoError(t, err)
	return branch.Volume.RawStore
}

type side struct {
	space branches.Space
	srv   *Service
	swarm p2p.SecureAskSwarm[PeerID]
}

func newTestPair(t testing.TB) (s1, s2 *side) {
	srv := inet256client.NewTestService(t)
	key1 := p2ptest.NewTestKey(t, 0)
	key2 := p2ptest.NewTestKey(t, 1)
	s1 = newTestSide(t, srv, key1)
	s2 = newTestSide(t, srv, key2)
	return s1, s2
}

func newTestSide(t testing.TB, inetSrv inet256.Service, privKey p2p.PrivateKey) *side {
	node, err := inetSrv.Open(context.Background(), privKey)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, node.Close())
	})
	swarm := mbapp.New(inet256client.NewSwarm(node), MaxMessageSize)
	newStore := func() cadata.Store { return cadata.NewMem(cadata.DefaultHash, MaxMessageSize) }
	space := branches.NewMem(newStore, cells.NewMem)
	srv := New(Params{
		Open:  func(PeerID) branches.Space { return space },
		Swarm: swarm,
	})
	return &side{
		space: space,
		srv:   srv,
		swarm: swarm,
	}
}
