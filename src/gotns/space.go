package gotns

import (
	"context"
	"encoding/json"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/branches"
)

var _ branches.Space = &Space{}

// Space implements branches.Space
type Space struct {
	client *Client
	volh   blobcache.Handle
}

func NewSpace(client *Client, volh blobcache.Handle) *Space {
	return &Space{client: client, volh: volh}
}

func (bs *Space) Create(ctx context.Context, name string, config branches.Config) (*branches.Info, error) {
	info := branches.Info{
		Salt:        config.Salt,
		Annotations: config.Annotations,
	}
	aux, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}
	if err := bs.client.CreateAt(ctx, bs.volh, name, aux); err != nil {
		return nil, err
	}
	return &info, nil
}

func (bs *Space) Get(ctx context.Context, name string) (*branches.Info, error) {
	ent, err := bs.client.GetEntry(ctx, bs.volh, name)
	if err != nil {
		return nil, err
	}
	if ent == nil {
		return nil, branches.ErrNotExist
	}
	return &branches.Info{}, nil
}

func (bs *Space) Open(ctx context.Context, name string) (branches.Volume, error) {
	return bs.client.OpenAt(ctx, bs.volh, name)
}

func (bs *Space) Delete(ctx context.Context, name string) error {
	return bs.client.DeleteEntry(ctx, bs.volh, name)
}

func (bs *Space) List(ctx context.Context, span branches.Span, limit int) ([]string, error) {
	return bs.client.ListEntries(ctx, bs.volh, span, limit)
}

func (bs *Space) Set(ctx context.Context, name string, config branches.Config) error {
	return fmt.Errorf("not implemented")
}
