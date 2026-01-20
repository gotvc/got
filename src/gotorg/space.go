package gotorg

import (
	"context"
	"encoding/json"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/internal/marks"
	"go.brendoncarroll.net/tai64"
)

var _ marks.Space = &Space{}

// Space implements marks.Space
type Space struct {
	client *Client
	volh   blobcache.Handle
}

func NewSpace(client *Client, volh blobcache.Handle) *Space {
	return &Space{client: client, volh: volh}
}

func (bs *Space) Create(ctx context.Context, name string, md marks.Metadata) (*marks.Info, error) {
	info := marks.Info{
		Config:      md.Config,
		Annotations: md.Annotations,
		CreatedAt:   tai64.Now().TAI64(),
	}
	aux, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}
	if err := bs.client.CreateAlias(ctx, bs.volh, name, aux); err != nil {
		return nil, err
	}
	return &info, nil
}

func (bs *Space) Inspect(ctx context.Context, name string) (*marks.Info, error) {
	ent, err := bs.client.GetAlias(ctx, bs.volh, name)
	if err != nil {
		return nil, err
	}
	if ent == nil {
		return nil, marks.ErrNotExist
	}
	return &marks.Info{}, nil
}

func (bs *Space) Open(ctx context.Context, name string) (*marks.Mark, error) {
	vol, err := bs.client.OpenAt(ctx, bs.volh, name, bs.client.ActAs, false)
	if err != nil {
		return nil, err
	}
	info, err := bs.Inspect(ctx, name)
	if err != nil {
		return nil, err
	}
	return &marks.Mark{
		Volume: vol,
		Info:   *info,
	}, nil
}

func (bs *Space) Delete(ctx context.Context, name string) error {
	return bs.client.DeleteAlias(ctx, bs.volh, name)
}

func (bs *Space) List(ctx context.Context, span marks.Span, limit int) ([]string, error) {
	return bs.client.ListAliases(ctx, bs.volh, span, limit)
}

func (bs *Space) Set(ctx context.Context, name string, config marks.Metadata) error {
	return fmt.Errorf("not implemented")
}
