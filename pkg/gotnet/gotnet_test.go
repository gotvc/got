package gotnet

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/p2p"
	"go.brendoncarroll.net/p2p/p/mbapp"
	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/state/cadata/storetest"
	"go.brendoncarroll.net/state/cells/celltest"
	"go.inet256.org/inet256/pkg/inet256"
	"go.inet256.org/inet256/pkg/inet256mem"
	"go.inet256.org/inet256/pkg/inet256test"
	"go.inet256.org/inet256/pkg/p2padapter"

	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/cells"
	"github.com/gotvc/got/pkg/stores"
	"github.com/gotvc/got/pkg/testutil"
)

func TestSpace(t *testing.T) {
	branches.TestSpace(t, func(t testing.TB) branches.Space {
		ctx := testutil.Context(t)
		s1, s2 := newTestPair(t)
		go s1.srv.Serve(ctx)
		go s2.srv.Serve(ctx)
		peer2 := s2.swarm.LocalAddrs()[0]
		return s1.srv.GetSpace(peer2)
	})
}

func TestStore(t *testing.T) {
	// TODO: MaxSize occasionally hangs during PullFrom
	t.Skip()
	storetest.TestStore(t, func(t testing.TB) cadata.Store {
		ctx := testutil.Context(t)
		s1, s2 := newTestPair(t)
		go s1.srv.Serve(ctx)
		go s2.srv.Serve(ctx)
		peer2 := s2.swarm.LocalAddrs()[0]
		space := s1.srv.GetSpace(peer2)
		return createStore(t, space)
	})
}

func TestCell(t *testing.T) {
	celltest.TestBytesCell(t, func(t testing.TB) cells.Cell {
		ctx := testutil.Context(t)
		s1, s2 := newTestPair(t)
		go s1.srv.Serve(ctx)
		go s2.srv.Serve(ctx)
		peer2 := s2.swarm.LocalAddrs()[0]
		space := s1.srv.GetSpace(peer2)
		return createCell(t, space)
	})
}

func createCell(t testing.TB, space branches.Space) cells.Cell {
	name := "test"
	ctx := testutil.Context(t)
	_, err := space.Create(ctx, name, branches.Config{})
	require.NoError(t, err)
	v, err := space.Open(ctx, name)
	require.NoError(t, err)
	return v.Cell
}

func createStore(t testing.TB, space branches.Space) cadata.Store {
	name := "test"
	ctx := testutil.Context(t)
	_, err := space.Create(ctx, name, branches.Config{})
	require.NoError(t, err)
	v, err := space.Open(ctx, name)
	require.NoError(t, err)
	return v.RawStore
}

type side struct {
	space branches.Space
	srv   *Service
	swarm p2p.SecureAskSwarm[PeerID, inet256.PublicKey]
}

func newTestPair(t testing.TB) (s1, s2 *side) {
	srv := inet256mem.New(inet256mem.WithQueueLen(10))
	key1 := inet256test.NewPrivateKey(t, 0)
	key2 := inet256test.NewPrivateKey(t, 1)
	s1 = newTestSide(t, srv, key1)
	s2 = newTestSide(t, srv, key2)
	return s1, s2
}

func newTestSide(t testing.TB, inetSrv inet256.Service, privKey inet256.PrivateKey) *side {
	ctx := testutil.Context(t)
	node, err := inetSrv.Open(ctx, privKey)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, node.Close())
	})
	swarm := mbapp.New(p2padapter.SwarmFromNode(node), MaxMessageSize)
	newStore := func() cadata.Store { return stores.NewMem() }
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
