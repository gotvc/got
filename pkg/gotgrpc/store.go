package gotgrpc

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/brendoncarroll/go-state/cadata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/stores"
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
		return cadata.ID{}, fmt.Errorf("bad ID from store. HAVE: %v WANT %v", res.Id, expected)
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
		switch status.Code(err) {
		case codes.NotFound:
			if errorMsgContains(err, "blob") {
				return 0, cadata.ErrNotFound
			}
		}
		return 0, err
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
	switch status.Code(err) {
	case codes.NotFound:
		if errorMsgContains(err, "blob") {
			return cadata.ErrNotFound
		}
	}
	return err
}

func (s Store) List(ctx context.Context, span cadata.Span, ids []cadata.ID) (int, error) {
	first := stores.FirstFromSpan(span)
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
		if span.Compare(ids[i], func(a, b cadata.ID) int { return a.Compare(b) }) < 0 {
			n = i
			break
		}
	}
	return n, err
}

func (s Store) MaxSize() int {
	return gotfs.DefaultMaxBlobSize
}

func (s Store) Hash(x []byte) cadata.ID {
	return gdat.Hash(x)
}
