package gotgrpc

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/brendoncarroll/go-state/cadata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotfs"
)

var _ cadata.Store = &Store{}

type Store struct {
	c   GotSpaceClient
	key string
	st  StoreType
}

func (s Store) Post(ctx context.Context, data []byte) (cadata.ID, error) {
	res, err := s.c.PostBlob(ctx, &PostBlobReq{
		Key:       s.key,
		StoreType: s.st,
		Data:      data,
	})
	expected := s.Hash(data)
	if err != nil {
		return cadata.ID{}, err
	}
	if !bytes.Equal(expected[:], res.Id) {
		return cadata.ID{}, errors.New("bad ID from store")
	}
	return expected, nil
}

func (s Store) Get(ctx context.Context, id cadata.ID, buf []byte) (int, error) {
	res, err := s.c.GetBlob(ctx, &GetBlobReq{
		Id:        id[:],
		Key:       s.key,
		StoreType: s.st,
	})
	// TODO: transform errors
	if err != nil {
		return 0, s.transformError(err)
	}
	if len(res.Data) > len(buf) {
		return 0, io.ErrShortBuffer
	}
	return copy(buf, res.Data), nil
}

func (s Store) Delete(ctx context.Context, id cadata.ID) error {
	_, err := s.c.DeleteBlob(ctx, &DeleteBlobReq{
		Key:       s.key,
		StoreType: s.st,
		Id:        id[:],
	})
	return err
}

func (s Store) Add(ctx context.Context, id cadata.ID) error {
	_, err := s.c.AddBlob(ctx, &AddBlobReq{
		Key:       s.key,
		StoreType: s.st,
		Id:        id[:],
	})
	return err
}

func (s Store) List(ctx context.Context, first cadata.ID, ids []cadata.ID) (int, error) {
	res, err := s.c.ListBlob(ctx, &ListBlobReq{
		Key:       s.key,
		StoreType: s.st,
		Begin:     first[:],
		Limit:     uint32(len(ids)),
	})
	if err != nil {
		return 0, err
	}
	var n int
	for i := range res.Ids {
		if n >= len(ids) {
			return n, io.ErrShortBuffer
		}
		ids[i] = cadata.IDFromBytes(res.Ids[i])
	}
	if res.IsEnd {
		err = cadata.ErrEndOfList
	}
	return n, err
}

func (s Store) MaxSize() int {
	return gotfs.DefaultMaxBlobSize
}

func (s Store) Hash(x []byte) cadata.ID {
	return gdat.Hash(x)
}

func (s Store) transformError(x error) error {
	switch {
	case x == nil:
		return nil
	case status.Code(x) == codes.NotFound:
		return cadata.ErrNotFound
	default:
		return x
	}
}
