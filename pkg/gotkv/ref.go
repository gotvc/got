package gotkv

import (
	"context"

	"github.com/blobcache/blobcache/pkg/blobs"
)

type Ref struct {
	CID blobs.ID
}

func PostRaw(ctx context.Context, s Store, data []byte) (*Ref, error) {
	id, err := s.Post(ctx, data)
	if err != nil {
		return nil, err
	}
	return &Ref{CID: id}, nil
}

func GetRawF(ctx context.Context, s Store, ref Ref, fn func(data []byte) error) error {
	return s.GetF(ctx, ref.CID, fn)
}
