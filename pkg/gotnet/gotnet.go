package gotnet

import (
	"context"
	"encoding/json"

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

// OpenFunc is the type a function which returns a view of a Space
// based on a PeerID
type OpenFunc = func(PeerID) branches.Space

type Params struct {
	Swarm  p2p.SecureAskSwarm
	Open   OpenFunc
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
	reqData := marshal(req)
	respData := make([]byte, MaxMessageSize)
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
