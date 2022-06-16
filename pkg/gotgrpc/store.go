package gotgrpc

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ cadata.Store = &Store{}

type Store struct {
	c   GotSpaceClient
	key string
	st  StoreType
}

func (s Store) Post(ctx context.Context, data []byte) (cadata.ID, error) {
	if len(data) > s.MaxSize() {
		return cadata.ID{}, cadata.ErrTooLarge
	}
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
	first := cadata.BeginFromSpan(span)
	req := &ListBlobReq{
		Key:       s.key,
		StoreType: s.st,
		Begin:     first[:],
		Limit:     uint32(len(ids)),
	}
	end, ok := cadata.EndFromSpan(span)
	if ok {
		req.End = end[:]
	}
	res, err := s.c.ListBlob(ctx, req)
	if err != nil {
		return 0, err
	}
	var n int
	for i := range res.Ids {
		if n >= len(ids) {
			break
		}
		id := cadata.IDFromBytes(res.Ids[i])
		if !span.Contains(id, func(a, b cadata.ID) int { return a.Compare(b) }) {
			logrus.Warnf("gotgrpc: store returned ID %v not in Span %v", id, span)
			continue
		}
		ids[i] = id
		n++
	}
	return n, err
}

func (s Store) MaxSize() int {
	return MaxBlobSize
}

func (s Store) Hash(x []byte) cadata.ID {
	return Hash(x)
}
