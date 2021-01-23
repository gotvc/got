package gotnet

import (
	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/s/peerswarm"
	"github.com/brendoncarroll/got/pkg/p2pkv"
)

type Client struct {
	client p2pkv.Client
}

func NewClient(swarm peerswarm.AskSwarm, dst p2p.PeerID) *Client {
	return &Client{
		client: p2pkv.NewClient(swarm, dst),
	}
}
