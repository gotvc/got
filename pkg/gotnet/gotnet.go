package gotnet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/cells"
	"github.com/gotvc/got/pkg/gotauthz"
	"github.com/gotvc/got/pkg/gotfs"
	"go.brendoncarroll.net/p2p"
	"go.brendoncarroll.net/p2p/p/p2pmux"
	"go.brendoncarroll.net/state/cadata"
	"go.inet256.org/inet256/pkg/inet256"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
)

const (
	channelBlobPull = "got/blob-pull@v0"
	channelBlobMain = "got/blob-main@v0"
	channelCell     = "got/cell@v0"
	channelSpace    = "got/space@v0"
)

const (
	opCreate = "CREATE"
	opGet    = "GET"
	opPost   = "POST"
	opList   = "LIST"
	opExists = "EXISTS"
	opDelete = "DELETE"
)

const (
	MaxMessageSize = gotfs.DefaultMaxBlobSize
)

type PeerID = inet256.Addr

// OpenFunc is the type a function which returns a view of a Space
// based on a PeerID
type OpenFunc = func(PeerID) branches.Space

type Params struct {
	Swarm p2p.SecureAskSwarm[PeerID, inet256.PublicKey]
	Open  OpenFunc
}

type Service struct {
	mux p2pmux.SecureAskMux[PeerID, string, inet256.PublicKey]

	blobPullSrv *blobPullSrv
	blobMainSrv *blobMainSrv
	cellSrv     *cellSrv
	spaceSrv    *spaceSrv
}

func New(params Params) *Service {
	mux := p2pmux.NewStringSecureAskMux(params.Swarm)
	srv := &Service{
		mux: mux,
	}
	srv.blobPullSrv = newBlobPullSrv(params.Open, newTempStore(), mux.Open(channelBlobPull))
	srv.blobMainSrv = newBlobMainSrv(params.Open, srv.blobPullSrv, mux.Open(channelBlobMain))
	srv.cellSrv = newCellSrv(params.Open, mux.Open(channelCell))
	srv.spaceSrv = newSpaceSrv(params.Open, mux.Open(channelSpace))
	return srv
}

func (s *Service) Serve(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return s.blobPullSrv.Serve(ctx)
	})
	eg.Go(func() error {
		return s.blobMainSrv.Serve(ctx)
	})
	eg.Go(func() error {
		return s.cellSrv.Serve(ctx)
	})
	eg.Go(func() error {
		return s.spaceSrv.Serve(ctx)
	})
	return eg.Wait()
}

func (s *Service) GetSpace(peer PeerID) branches.Space {
	newCell := func(cid CellID) cells.Cell {
		return newCell(s.cellSrv, cid)
	}
	newStore := func(sid StoreID) cadata.Store {
		return newStore(s.blobMainSrv, s.blobPullSrv, sid)
	}
	return newSpace(s.spaceSrv, peer, newCell, newStore)
}

func askJson(ctx context.Context, s p2p.Asker[PeerID], dst PeerID, resp, req interface{}) error {
	reqData := marshal(req)
	respData := make([]byte, MaxMessageSize)
	n, err := s.Ask(ctx, respData, dst, p2p.IOVec{reqData})
	if err != nil {
		return err
	}
	return json.Unmarshal(respData[:n], resp)
}

func serveAsks(ctx context.Context, x p2p.AskSwarm[PeerID], fn func(context.Context, []byte, p2p.Message[PeerID]) int) error {
	eg := errgroup.Group{}
	eg.Go(func() error {
		return p2p.DiscardTells[PeerID](ctx, x)
	})
	eg.Go(func() error {
		for {
			if err := x.ServeAsk(ctx, fn); err != nil {
				return err
			}
		}
	})
	return eg.Wait()
}

func marshal(x interface{}) []byte {
	data, err := json.Marshal(x)
	if err != nil {
		panic(err)
	}
	return data
}

func unmarshal(buf []byte, x interface{}) error {
	return json.Unmarshal(buf, x)
}

type WireError struct {
	Code    codes.Code
	Message string
}

func (e WireError) Error() string {
	return fmt.Sprintf("{%v: %v}", e.Code, e.Message)
}

func parseWireError(err WireError) error {
	switch {
	case err.Code == codes.NotFound && strings.Contains(err.Message, "branch"):
		return branches.ErrNotExist
	case err.Code == codes.AlreadyExists && strings.Contains(err.Message, "branch"):
		return branches.ErrExists
	case err.Code == codes.PermissionDenied:
		// TODO: parse the error string
		return gotauthz.ErrNotAllowed{}
	case err.Code == codes.InvalidArgument && strings.Contains(err.Message, cadata.ErrTooLarge.Error()):
		return cadata.ErrTooLarge
	default:
		return err
	}
}

func makeWireError(err error) *WireError {
	switch {
	case errors.Is(err, branches.ErrNotExist):
		return &WireError{
			Code:    codes.NotFound,
			Message: err.Error(),
		}
	case errors.Is(err, branches.ErrExists):
		return &WireError{
			Code:    codes.AlreadyExists,
			Message: err.Error(),
		}
	case errors.As(err, &gotauthz.ErrNotAllowed{}):
		return &WireError{
			Code:    codes.PermissionDenied,
			Message: err.Error(),
		}
	case errors.Is(err, cadata.ErrTooLarge):
		return &WireError{
			Code:    codes.InvalidArgument,
			Message: err.Error(),
		}
	default:
		return &WireError{
			Code:    codes.Unknown,
			Message: err.Error(),
		}
	}
}
