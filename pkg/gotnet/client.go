package gotnet

import (
	"context"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/s/peerswarm"
	"github.com/brendoncarroll/got/pkg/p2pkv"
	"github.com/brendoncarroll/got/pkg/realms"
)

var _ realms.Realm = &Client{}

type Client struct {
	client p2pkv.Client
}

func NewClient(swarm peerswarm.AskSwarm, dst p2p.PeerID) *Client {
	return &Client{
		client: p2pkv.NewClient(swarm, dst),
	}
}

func (c *Client) Get(ctx context.Context, k string) (*realms.Volume, error) {
	panic("not implemented")
}

func (c *Client) Create(ctx context.Context, k string) error {
	panic("not implemented")
}

func (c *Client) Delete(ctx context.Context, k string) error {
	panic("not implemented")
}

func (c *Client) List(ctx context.Context, k string) ([]string, error) {
	panic("not implemented")
}
