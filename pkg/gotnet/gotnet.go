package gotnet

import (
	"context"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/stringmux"
	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/volumes"
	"golang.org/x/sync/errgroup"
)

const (
	channelBlobPull = "got/blob-pull@v0"
	channelBlobMain = "got/blob-main@v0"
)

type Repo interface {
	GetRealm() volumes.Realm
	GetACL() ACL
}

type ACL interface {
	CanWriteAny(p2p.PeerID) bool
	CanReadAny(p2p.PeerID) bool
	CanWrite(id p2p.PeerID, name string) bool
	CanRead(id p2p.PeerID, name string) bool
}

type Params struct {
	Store cadata.Store
	Mux   stringmux.AskMux
	ACL   ACL
}

type Service struct {
	mux stringmux.AskMux

	blobPullSrv *blobPullSrv
	blobMainSrv *blobMainSrv
}

func New(params Params) *Service {
	srv := &Service{
		mux: params.Mux,
	}
	srv.blobPullSrv = newBlobPullSrv(params.Store, params.ACL, params.Mux.Open(channelBlobPull))
	srv.blobMainSrv = newBlobMainSrv(params.Store, srv.blobPullSrv, params.ACL, params.Mux.Open(channelBlobMain))
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

func (s *Service) GetRealm(peer p2p.PeerID) volumes.Realm {
	return &realm{
		s:    s,
		peer: peer,
	}
}

func (s *Service) Close() error {
	return nil
}

var _ volumes.Realm = &realm{}

type realm struct {
	s    *Service
	peer p2p.PeerID
}

func (r *realm) Create(ctx context.Context, name string) error {
	return nil
}

func (r *realm) Get(ctx context.Context, name string) (*volumes.Volume, error) {
	return nil, nil
}

func (r *realm) Delete(ctx context.Context, name string) error {
	return nil
}

func (r *realm) List(ctx context.Context, prefix string) ([]string, error) {
	return nil, nil
}
