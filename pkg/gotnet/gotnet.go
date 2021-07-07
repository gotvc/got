package gotnet

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/stringmux"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/got/pkg/branches"
	"github.com/brendoncarroll/got/pkg/cells"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const (
	channelBlobPull = "got/blob-pull@v0"
	channelBlobMain = "got/blob-main@v0"
	channelCell     = "got/cell@v0"
	channelRealm    = "got/realm@v0"
)

const (
	opCreate = "CREATE"
	opPush   = "PUSH"
	opList   = "LIST"
	opExists = "EXISTS"
	opDelete = "DELETE"
)

type ErrNotAllowed struct {
	Subject      p2p.PeerID
	Verb, Object string
}

func (e ErrNotAllowed) Error() string {
	return fmt.Sprintf("%v cannot perform %s on %s", e.Subject, e.Object, e.Verb)
}

type ACL interface {
	CanWriteAny(p2p.PeerID) bool
	CanReadAny(p2p.PeerID) bool
	CanWrite(id p2p.PeerID, name string) bool
	CanRead(id p2p.PeerID, name string) bool
}

type Params struct {
	Mux    stringmux.SecureAskMux
	ACL    ACL
	Realm  branches.Realm
	Logger *logrus.Logger
}

type Service struct {
	mux stringmux.SecureAskMux

	blobPullSrv *blobPullSrv
	blobMainSrv *blobMainSrv
	cellSrv     *cellSrv
	realmSrv    *realmSrv
}

func New(params Params) *Service {
	srv := &Service{
		mux: params.Mux,
	}
	srv.blobPullSrv = newBlobPullSrv(newTempStore(), params.ACL, params.Mux.Open(channelBlobPull))
	srv.blobMainSrv = newBlobMainSrv(srv.blobPullSrv, params.ACL, params.Mux.Open(channelBlobMain))
	srv.cellSrv = newCellSrv(params.Realm, params.ACL, params.Mux.Open(channelCell))
	srv.realmSrv = newRealmSrv(params.Realm, params.ACL, params.Mux.Open(channelRealm))
	return srv
}

func (s *Service) Serve() error {
	eg := errgroup.Group{}
	eg.Go(func() error {
		return s.blobMainSrv.Serve()
	})
	eg.Go(func() error {
		return s.blobPullSrv.Serve()
	})
	return eg.Wait()
}

func (s *Service) GetRealm(peer p2p.PeerID) branches.Realm {
	newCell := func(cid CellID) cells.Cell {
		return newCell(s.cellSrv, cid)
	}
	newStore := func(sid StoreID) cadata.Store {
		return newStore(s.blobMainSrv, s.blobPullSrv, sid)
	}
	return newRealm(s.realmSrv, peer, newCell, newStore)

}

func askJson(ctx context.Context, s p2p.Asker, dst p2p.PeerID, resp, req interface{}) error {
	reqData, err := json.Marshal(req)
	if err != nil {
		return err
	}
	respData, err := s.Ask(ctx, dst, p2p.IOVec{reqData})
	if err != nil {
		return err
	}
	return json.Unmarshal(respData, resp)
}
