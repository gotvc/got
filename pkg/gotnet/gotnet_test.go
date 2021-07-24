package gotnet

import (
	"crypto/ed25519"
	"testing"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/p2pmux"
	"github.com/brendoncarroll/go-p2p/s/peerswarm"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/cells"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/inet256/inet256/pkg/inet256p2p"
	"github.com/stretchr/testify/require"
)

func TestRealm(t *testing.T) {
	branches.TestRealm(t, func(t testing.TB) branches.Realm {
		s1, s2 := newTestPair(t)
		go s1.srv.Serve()
		go s2.srv.Serve()
		peer2 := s2.swarm.LocalAddrs()[0].(p2p.PeerID)
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
	s1 = newTestSide(t, key1)
	s2 = newTestSide(t, key2)
	return s1, s2
}

func newTestSide(t testing.TB, srv inet256.Service, privKey p2p.PrivateKey) *side {
	swarm, err := inet256p2p.NewSwarm("127.0.0.1:25600", privKey)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, swarm.Close())
	})
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

func (allACL) CanRead(p2p.PeerID, string) bool  { return true }
func (allACL) CanReadAny(p2p.PeerID) bool       { return true }
func (allACL) CanWrite(p2p.PeerID, string) bool { return true }
func (allACL) CanWriteAny(p2p.PeerID) bool      { return true }
