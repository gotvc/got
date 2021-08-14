package gotnet

import (
	"context"
	"crypto/ed25519"
	"testing"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/mbapp"
	"github.com/brendoncarroll/go-p2p/p/p2pmux"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/cells"
	"github.com/inet256/inet256/client/go_client/inet256client"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/stretchr/testify/require"
)

func TestRealm(t *testing.T) {
	branches.TestRealm(t, func(t testing.TB) branches.Realm {
		s1, s2 := newTestPair(t)
		go s1.srv.Serve()
		go s2.srv.Serve()
		peer2 := s2.swarm.LocalAddrs()[0].(PeerID)
		return s1.srv.GetRealm(peer2)
	})
}

type side struct {
	realm branches.Realm
	srv   *Service
	swarm p2p.SecureAskSwarm
}

func newTestPair(t testing.TB) (s1, s2 *side) {
	srv := inet256client.NewTestService(t)
	_, key1, _ := ed25519.GenerateKey(nil)
	_, key2, _ := ed25519.GenerateKey(nil)
	s1 = newTestSide(t, srv, key1)
	s2 = newTestSide(t, srv, key2)
	return s1, s2
}

func newTestSide(t testing.TB, inetSrv inet256.Service, privKey p2p.PrivateKey) *side {
	node, err := inetSrv.CreateNode(context.Background(), privKey)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, node.Close())
	})
	swarm := mbapp.New(inet256client.NewSwarm(node, privKey.Public()), MaxMessageSize)
	newStore := func() cadata.Store { return cadata.NewMem(1 << 20) }
	realm := branches.NewMem(newStore, cells.NewMem)
	srv := New(Params{
		Realm: realm,
		ACL:   newAllACL(),
		Mux:   p2pmux.NewStringSecureAskMux(swarm),
	})
	return &side{
		realm: realm,
		srv:   srv,
		swarm: swarm,
	}
}

type allACL struct{}

func newAllACL() ACL {
	return allACL{}
}

func (allACL) CanRead(PeerID, string) bool  { return true }
func (allACL) CanReadAny(PeerID) bool       { return true }
func (allACL) CanWrite(PeerID, string) bool { return true }
func (allACL) CanWriteAny(PeerID) bool      { return true }
