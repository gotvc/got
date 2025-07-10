package quichub

import (
	"context"
	"fmt"
	"net"
	"sync"

	"go.brendoncarroll.net/p2p"
	"go.brendoncarroll.net/p2p/f/x509"
	"go.brendoncarroll.net/p2p/s/quicswarm"
	"go.brendoncarroll.net/p2p/s/udpswarm"
	"go.inet256.org/inet256/pkg/inet256"
	"go.inet256.org/inet256/pkg/mesh256"

	"github.com/gotvc/got/src/gotfs"
)

var quicOpts = []quicswarm.Option[udpswarm.Addr]{
	quicswarm.WithFingerprinter[udpswarm.Addr](func(pubKey x509.PublicKey) p2p.PeerID {
		pubKey2, err := inet256.PublicKeyFromBuiltIn(pubKey)
		if err != nil {
			return p2p.PeerID{}
		}
		return p2p.PeerID(inet256.NewAddr(pubKey2))
	}),
	quicswarm.WithMTU[udpswarm.Addr](gotfs.DefaultMaxBlobSize),
}

func Dial(privateKey inet256.PrivateKey, id inet256.ID, addr string) (p2p.SecureAskSwarm[inet256.Addr, inet256.PublicKey], error) {
	ua, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	privX509, err := convertPrivateKey(privateKey)
	if err != nil {
		return nil, err
	}
	sw, err := quicswarm.NewOnUDP("0.0.0.0:", privX509, quicOpts...)
	if err != nil {
		return nil, err
	}
	return &QUICHub{
		sw:       sw,
		isClient: true,
		directory: map[inet256.Addr]udpswarm.Addr{
			id: udpswarm.FromNetAddr(*ua),
		},
	}, nil
}

func Listen(privateKey inet256.PrivateKey, addr string) (p2p.SecureAskSwarm[inet256.Addr, inet256.PublicKey], error) {
	privX509, err := convertPrivateKey(privateKey)
	if err != nil {
		return nil, err
	}
	sw, err := quicswarm.NewOnUDP(addr, privX509, quicOpts...)
	if err != nil {
		return nil, err
	}
	return &QUICHub{
		sw:        sw,
		isClient:  false,
		directory: make(map[inet256.Addr]udpswarm.Addr),
	}, nil
}

var _ p2p.SecureAskSwarm[inet256.Addr, inet256.PublicKey] = &QUICHub{}

// QUICHub provides a p2p.SecureAskSwarm[inet256.Addr] using QUIC.
type QUICHub struct {
	sw       *quicswarm.Swarm[udpswarm.Addr]
	isClient bool

	mu        sync.Mutex
	directory map[inet256.Addr]udpswarm.Addr
}

func (qh *QUICHub) Ask(ctx context.Context, resp []byte, dst inet256.Addr, req p2p.IOVec) (int, error) {
	dst2, err := qh.pickAddr(dst)
	if err != nil {
		return 0, err
	}
	return qh.sw.Ask(ctx, resp, *dst2, req)
}

func (qh *QUICHub) Tell(ctx context.Context, dst inet256.Addr, data p2p.IOVec) error {
	dst2, err := qh.pickAddr(dst)
	if err != nil {
		return err
	}
	return qh.sw.Tell(ctx, *dst2, data)
}

func (qh *QUICHub) Receive(ctx context.Context, fn func(p2p.Message[inet256.Addr])) error {
	return qh.sw.Receive(ctx, func(m p2p.Message[quicswarm.Addr[udpswarm.Addr]]) {
		fn(qh.upwardMessage(m))
	})
}

func (qh *QUICHub) ServeAsk(ctx context.Context, fn func(context.Context, []byte, p2p.Message[inet256.Addr]) int) error {
	return qh.sw.ServeAsk(ctx, func(ctx context.Context, resp []byte, req p2p.Message[quicswarm.Addr[udpswarm.Addr]]) int {
		return fn(ctx, resp, qh.upwardMessage(req))
	})
}

func (qh *QUICHub) Close() error {
	return qh.sw.Close()
}

func (qh *QUICHub) MTU() int {
	return qh.sw.MTU()
}

func (qh *QUICHub) LocalAddrs() []inet256.Addr {
	pubKey, err := mesh256.PublicKeyFromX509(qh.sw.PublicKey())
	if err != nil {
		// this is our key, so okay to panic.  This can't be triggered remotely
		panic(err)
	}
	return []inet256.Addr{inet256.NewAddr(pubKey)}
}

func (qh *QUICHub) LookupPublicKey(ctx context.Context, target inet256.Addr) (inet256.PublicKey, error) {
	addr, err := qh.pickAddr(target)
	if err != nil {
		return nil, err
	}
	pub, err := qh.sw.LookupPublicKey(ctx, *addr)
	if err != nil {
		return nil, err
	}
	return mesh256.PublicKeyFromX509(pub)
}

func (qh *QUICHub) ParseAddr(x []byte) (inet256.Addr, error) {
	return inet256.ParseAddrBase64(x)
}

func (qh *QUICHub) PublicKey() inet256.PublicKey {
	pub, err := mesh256.PublicKeyFromX509(qh.sw.PublicKey())
	if err != nil {
		panic(err)
	}
	return pub
}

func (qh *QUICHub) pickAddr(target inet256.Addr) (*quicswarm.Addr[udpswarm.Addr], error) {
	qh.mu.Lock()
	defer qh.mu.Unlock()
	addr, exists := qh.directory[target]
	if !exists {
		return nil, inet256.ErrAddrUnreachable{Addr: target}
	}
	return &quicswarm.Addr[udpswarm.Addr]{
		ID:   p2p.PeerID(target),
		Addr: udpswarm.Addr(addr),
	}, nil
}

func (qh *QUICHub) upwardMessage(m p2p.Message[quicswarm.Addr[udpswarm.Addr]]) p2p.Message[inet256.Addr] {
	src := m.Src
	dst := m.Dst
	srcID := inet256.ID(src.ID)
	dstID := inet256.ID(dst.ID)
	qh.directory[srcID] = src.Addr
	return p2p.Message[inet256.Addr]{
		Src:     srcID,
		Dst:     dstID,
		Payload: m.Payload,
	}
}

func convertPrivateKey(x inet256.PrivateKey) (x509.PrivateKey, error) {
	switch x := x.(type) {
	case *inet256.Ed25519PrivateKey:
		return x509.PrivateKey{
			Algorithm: x509.Algo_Ed25519,
			Data:      x.Seed()[:],
		}, nil
	default:
		return x509.PrivateKey{}, fmt.Errorf("cannot convert INET256 private key into x509 private key")
	}
}
