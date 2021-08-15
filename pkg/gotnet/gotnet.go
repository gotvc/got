package gotnet

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/p2pmux"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/cells"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
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
	MaxMessageSize = maxBlobSize
)

type PeerID = inet256.Addr

type ErrNotAllowed struct {
	Subject      PeerID
	Verb, Object string
}

func (e ErrNotAllowed) Error() string {
	return fmt.Sprintf("%v cannot perform %s on %s", e.Subject, e.Object, e.Verb)
}

type ACL interface {
	CanWriteAny(PeerID) bool
	CanReadAny(PeerID) bool
	CanWrite(id PeerID, name string) bool
	CanRead(id PeerID, name string) bool
}

type Params struct {
	Mux    p2pmux.StringSecureAskMux
	ACL    ACL
	Space  branches.Space
	Logger *logrus.Logger
}

type Service struct {
	mux p2pmux.StringSecureAskMux

	blobPullSrv *blobPullSrv
	blobMainSrv *blobMainSrv
	cellSrv     *cellSrv
	spaceSrv    *spaceSrv
}

func New(params Params) *Service {
	srv := &Service{
		mux: params.Mux,
	}
	srv.blobPullSrv = newBlobPullSrv(newTempStore(), params.ACL, params.Mux.Open(channelBlobPull))
	srv.blobMainSrv = newBlobMainSrv(params.Space, srv.blobPullSrv, params.ACL, params.Mux.Open(channelBlobMain))
	srv.cellSrv = newCellSrv(params.Space, params.ACL, params.Mux.Open(channelCell))
	srv.spaceSrv = newSpaceSrv(params.Space, params.ACL, params.Mux.Open(channelSpace))
	return srv
}

func (s *Service) Serve() error {
	ctx := context.Background()
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

func askJson(ctx context.Context, s p2p.Asker, dst PeerID, resp, req interface{}) error {
	reqData, err := json.Marshal(req)
	if err != nil {
		return err
	}
	respData := make([]byte, 1<<16)
	n, err := s.Ask(ctx, respData, dst, p2p.IOVec{reqData})
	if err != nil {
		return err
	}
	return json.Unmarshal(respData[:n], resp)
}

func serveAsks(ctx context.Context, x p2p.AskSwarm, fn p2p.AskHandler) error {
	eg := errgroup.Group{}
	eg.Go(func() error {
		return p2p.DiscardTells(ctx, x)
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
