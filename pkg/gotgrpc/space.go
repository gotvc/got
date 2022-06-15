package gotgrpc

import (
	"context"
	"strings"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-tai64"
	"google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"

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
		Key:  key,
		Salt: p.Salt,
	})
	if err != nil {
		switch status.Code(err) {
		case codes.AlreadyExists:
			if errorMsgContains(err, "branch") {
				return nil, branches.ErrExists
			}
		}
		return nil, err
	}
	return s.makeBranch(key, res), nil
}

func (s Space) Delete(ctx context.Context, key string) error {
	_, err := s.c.DeleteBranch(ctx, &DeleteBranchReq{Key: key})
	return err
}

func (s Space) Get(ctx context.Context, key string) (*branches.Branch, error) {
	res, err := s.c.GetBranch(ctx, &GetBranchReq{
		Key: key,
	})
	if err != nil {
		switch status.Code(err) {
		case codes.NotFound:
			if errorMsgContains(err, "branch") {
				return nil, branches.ErrNotExist
			}
		}
		return nil, err
	}
	return s.makeBranch(key, res), nil
}

func (s Space) List(ctx context.Context, span branches.Span, limit int) ([]string, error) {
	res, err := s.c.ListBranch(ctx, &ListBranchReq{
		Begin: span.Begin,
		End:   span.End,
	})
	if err != nil {
		return nil, err
	}
	return res.Keys, nil
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

func errorMsgContains(err error, sub string) bool {
	return strings.Contains(strings.ToLower(err.Error()), sub)
}
