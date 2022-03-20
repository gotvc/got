package quichub

import (
	"context"
	"net"
	"sync"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/s/quicswarm"
	"github.com/brendoncarroll/go-p2p/s/udpswarm"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/inet256/inet256/pkg/inet256"
)

var quicOpts = []quicswarm.Option[udpswarm.Addr]{
	quicswarm.WithFingerprinter[udpswarm.Addr](func(pubKey p2p.PublicKey) p2p.PeerID {
		return p2p.PeerID(inet256.NewAddr(pubKey))
	}),
	quicswarm.WithMTU[udpswarm.Addr](gotfs.DefaultMaxBlobSize),
}

func Dial(privateKey inet256.PrivateKey, id inet256.ID, addr string) (p2p.SecureAskSwarm[inet256.Addr], error) {
	ua, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	sw, err := quicswarm.NewOnUDP("0.0.0.0:", privateKey, quicOpts...)
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

func Listen(privateKey inet256.PrivateKey, addr string) (p2p.SecureAskSwarm[inet256.Addr], error) {
	sw, err := quicswarm.NewOnUDP(addr, privateKey, quicOpts...)
	if err != nil {
		return nil, err
	}
	return &QUICHub{
		sw:        sw,
		isClient:  false,
		directory: make(map[inet256.Addr]udpswarm.Addr),
	}, nil
}

var _ p2p.SecureAskSwarm[inet256.Addr] = &QUICHub{}

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

func (qh *QUICHub) MTU(ctx context.Context, target inet256.Addr) int {
	addr, err := qh.pickAddr(target)
	if err != nil {
		return quicswarm.DefaultMTU
	}
	return qh.sw.MTU(ctx, *addr)
}

func (qh *QUICHub) LocalAddrs() []inet256.Addr {
	return []inet256.Addr{inet256.NewAddr(qh.sw.PublicKey())}
}

func (qh *QUICHub) LookupPublicKey(ctx context.Context, target inet256.Addr) (p2p.PublicKey, error) {
	addr, err := qh.pickAddr(target)
	if err != nil {
		return nil, err
	}
	return qh.sw.LookupPublicKey(ctx, *addr)
}

func (qh *QUICHub) MaxIncomingSize() int {
	return qh.sw.MaxIncomingSize()
}

func (qh *QUICHub) ParseAddr(x []byte) (inet256.Addr, error) {
	return inet256.ParseAddrB64(x)
}

func (qh *QUICHub) PublicKey() p2p.PublicKey {
	return qh.sw.PublicKey()
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
	qh.directory[srcID] = src.Addr.(udpswarm.Addr)
	return p2p.Message[inet256.Addr]{
		Src:     srcID,
		Dst:     dstID,
		Payload: m.Payload,
	}
}
