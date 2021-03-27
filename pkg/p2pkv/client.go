package p2pkv

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/s/peerswarm"
)

type Client interface {
	Get(ctx context.Context, col string, key []byte) ([]byte, error)
	Post(ctx context.Context, col string, value []byte) ([]byte, error)
	CAS(ctx context.Context, col string, key, prevSum, nextValue []byte) ([]byte, error)
	Put(ctx context.Context, col string, key, value []byte) error
	Delete(ctx context.Context, col string, key []byte) error
}

type client struct {
	swarm peerswarm.AskSwarm
	dst   p2p.PeerID
}

func NewClient(swarm peerswarm.AskSwarm, dst p2p.PeerID) Client {
	return &client{
		swarm: swarm,
		dst:   dst,
	}
}

func (c *client) Get(ctx context.Context, col string, key []byte) ([]byte, error) {
	respData, err := c.ask(ctx, Request{
		Op: OpGet,
		Body: marshal(GetRequest{
			Key: key,
		}),
	})
	if err != nil {
		return nil, err
	}
	getResp := GetResponse{}
	if err := unmarshal(respData, getResp); err != nil {
		return nil, err
	}
	if !getResp.Exists {
		return nil, nil
	}
	return getResp.Value, nil
}

func (c *client) Post(ctx context.Context, col string, value []byte) ([]byte, error) {
	respData, err := c.ask(ctx, Request{
		Op: OpPost,
		Body: marshal(PostRequest{
			Value: value,
		}),
	})
	if err != nil {
		return nil, err
	}
	postResp := PostResponse{}
	if err := unmarshal(respData, &postResp); err != nil {
		return nil, err
	}
	return postResp.Key, nil
}

func (c *client) CAS(ctx context.Context, col string, key, prevSum, nextValue []byte) ([]byte, error) {
	respData, err := c.ask(ctx, Request{
		Op: OpCAS,
		Body: marshal(CASRequest{
			Key:       key,
			PrevSum:   prevSum,
			NextValue: nextValue,
		}),
	})
	if err != nil {
		return nil, err
	}
	casResp := CASResponse{}
	if err := unmarshal(respData, &casResp); err != nil {
		return nil, err
	}
	return casResp.Actual, nil
}

func (c *client) Put(ctx context.Context, col string, key, value []byte) error {
	respData, err := c.ask(ctx, Request{
		Op: OpCAS,
		Body: marshal(PutRequest{
			Key:   key,
			Value: value,
		}),
	})
	if err != nil {
		return err
	}
	putResp := PutResponse{}
	if err := unmarshal(respData, &putResp); err != nil {
		return err
	}
	return nil
}

func (c *client) Delete(ctx context.Context, col string, key []byte) error {
	respData, err := c.ask(ctx, Request{
		Op: OpCAS,
		Body: marshal(DeleteRequest{
			Key: key,
		}),
	})
	if err != nil {
		return err
	}
	deleteResp := DeleteResponse{}
	if err := unmarshal(respData, &deleteResp); err != nil {
		return err
	}
	return nil
}

func (c *client) ask(ctx context.Context, req Request) ([]byte, error) {
	if _, err := rand.Reader.Read(req.ID[:]); err != nil {
		return nil, err
	}
	reqData, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	respData, err := c.swarm.Ask(ctx, c.dst, p2p.IOVec{reqData})
	if err != nil {
		return nil, err
	}
	resp := Response{}
	if err := json.Unmarshal(respData, &resp); err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}
	return resp.Success, nil
}
