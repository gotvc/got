package gotgrpc

import (
	"context"
	"errors"
	"io"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-tai64"

	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/cells"
)

var _ branches.Space = Space{}

type Space struct {
	c GotSpaceClient
}

func NewSpace(c GotSpaceClient) Space {
	return Space{c}
}

func (s Space) Create(ctx context.Context, key string, p branches.Params) (*branches.Branch, error) {
	res, err := s.c.CreateBranch(ctx, &CreateBranchReq{
		Salt: p.Salt,
	})
	if err != nil {
		return nil, err
	}
	return s.makeBranch(key, res), nil
}

func (s Space) Delete(ctx context.Context, key string) error {
	_, err := s.c.DeleteBranch(ctx, &DeleteBranchReq{Key: key})
	return err
}

func (s Space) Get(ctx context.Context, key string) (*branches.Branch, error) {
	res, err := s.c.GetBranch(ctx, &GetBranchReq{})
	if err != nil {
		return nil, err
	}
	return s.makeBranch(key, res), nil
}

func (s Space) ForEach(ctx context.Context, span branches.Span, fn func(string) error) error {
	client, err := s.c.ListBranch(ctx, &ListBranchReq{
		Begin: span.Begin,
		End:   span.End,
	})
	if err != nil {
		return err
	}
	for {
		res, err := client.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if err := fn(res.Key); err != nil {
			return err
		}
	}
	return nil
}

func (s Space) makeBranch(key string, bi *BranchInfo) *branches.Branch {
	createdAt, _ := tai64.Parse(bi.CreatedAt)
	return &branches.Branch{
		Salt:        bi.Salt,
		CreatedAt:   createdAt,
		Annotations: bi.Annotations,
		Volume:      s.makeVolume(key),
	}
}

func (s Space) makeVolume(key string) branches.Volume {
	return branches.Volume{
		Cell:     s.makeCell(key),
		VCStore:  s.makeStore(key, StoreType_VC),
		FSStore:  s.makeStore(key, StoreType_FS),
		RawStore: s.makeStore(key, StoreType_RAW),
	}
}

func (s Space) makeStore(key string, st StoreType) cadata.Store {
	return &Store{c: s.c, key: key, st: st}
}

func (s Space) makeCell(key string) cells.Cell {
	return &Cell{c: s.c, key: key}
}
