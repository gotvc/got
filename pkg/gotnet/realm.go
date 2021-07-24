package gotnet

import (
	"context"
	"encoding/json"
	"time"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/cells"
)

type BranchID struct {
	Peer p2p.PeerID
	Name string
}

type realmSrv struct {
	realm branches.Realm
	acl   ACL
	swarm p2p.AskSwarm
	log   *logrus.Logger
}

func newRealmSrv(realm branches.Realm, acl ACL, swarm p2p.AskSwarm) *realmSrv {
	return &realmSrv{
		realm: realm,
		acl:   acl,
		swarm: swarm,
		log:   logrus.StandardLogger(),
	}
}

func (srv *realmSrv) Serve(ctx context.Context) error {
	return serveAsks(ctx, srv.swarm, srv.handleAsk)
}

func (s *realmSrv) Create(ctx context.Context, bid BranchID) error {
	req := RealmReq{
		Op:   opCreate,
		Name: bid.Name,
	}
	var resp RealmRes
	if err := askJson(ctx, s.swarm, bid.Peer, &resp, &req); err != nil {
		return err
	}
	if resp.Error != nil {
		return errors.New(*resp.Error)
	}
	return nil
}

func (s *realmSrv) Delete(ctx context.Context, bid BranchID) error {
	req := RealmReq{
		Op:   opDelete,
		Name: bid.Name,
	}
	var resp RealmRes
	if err := askJson(ctx, s.swarm, bid.Peer, &resp, &req); err != nil {
		return err
	}
	if resp.Error != nil {
		return errors.New(*resp.Error)
	}
	return nil
}

func (s *realmSrv) Exists(ctx context.Context, bid BranchID) (bool, error) {
	req := RealmReq{
		Op:   opExists,
		Name: bid.Name,
	}
	var resp RealmRes
	if err := askJson(ctx, s.swarm, bid.Peer, &resp, &req); err != nil {
		return false, err
	}
	if resp.Error != nil {
		return false, errors.New(*resp.Error)
	}
	if resp.Exists == nil {
		return false, errors.Errorf("empty response")
	}
	return *resp.Exists, nil
}

func (s *realmSrv) ForEach(ctx context.Context, peer p2p.PeerID, fn func(string) error) error {
	req := RealmReq{
		Op: opList,
	}
	var resp RealmRes
	if err := askJson(ctx, s.swarm, peer, &resp, &req); err != nil {
		return err
	}
	if resp.Error != nil {
		return errors.New(*resp.Error)
	}
	for _, name := range resp.List {
		if err := fn(name); err != nil {
			return err
		}
	}
	return nil
}

func (s *realmSrv) handleAsk(resp []byte, msg p2p.Message) int {
	ctx, cf := context.WithTimeout(context.Background(), time.Minute)
	defer cf()
	peer := msg.Src.(p2p.PeerID)
	if !s.acl.CanReadAny(peer) && !s.acl.CanWriteAny(peer) {
		return 0
	}
	res, err := func() (*RealmRes, error) {
		var req RealmReq
		if err := json.Unmarshal(msg.Payload, &req); err != nil {
			return nil, err
		}
		s.log.Infof("%s from %v", req.Op, peer)
		switch req.Op {
		case opCreate:
			return s.handleCreate(ctx, peer, req.Name)
		case opDelete:
			return s.handleDelete(ctx, peer, req.Name)
		case opExists:
			return s.handleExists(ctx, peer, req.Name)
		case opList:
			return s.handleList(ctx, peer)
		default:
			return nil, errors.Errorf("unrecognized operation %s", req.Op)
		}
	}()
	if err != nil {
		logrus.Error(err)
		errMsg := err.Error()
		res = &RealmRes{
			Error: &errMsg,
		}
	}
	data, _ := json.Marshal(res)
	return copy(resp, data)
}

func (s *realmSrv) handleCreate(ctx context.Context, peer p2p.PeerID, name string) (*RealmRes, error) {
	if err := checkACL(s.acl, peer, name, true); err != nil {
		return nil, err
	}
	if err := s.realm.Create(ctx, name); err != nil {
		return nil, err
	}
	return &RealmRes{}, nil
}

func (s *realmSrv) handleDelete(ctx context.Context, peer p2p.PeerID, name string) (*RealmRes, error) {
	if err := checkACL(s.acl, peer, name, true); err != nil {
		return nil, err
	}
	if err := checkACL(s.acl, peer, name, true); err != nil {
		return nil, err
	}
	if err := s.realm.Delete(ctx, name); err != nil {
		return nil, err
	}
	return &RealmRes{}, nil
}

func (s *realmSrv) handleExists(ctx context.Context, peer p2p.PeerID, name string) (*RealmRes, error) {
	if err := checkACL(s.acl, peer, name, false); err != nil {
		return nil, err
	}
	_, err := s.realm.Get(ctx, name)
	if err != nil && err != branches.ErrNotExist {
		return nil, err
	}
	exists := err == nil
	return &RealmRes{
		Exists: &exists,
	}, nil
}

func (s *realmSrv) handleList(ctx context.Context, peer p2p.PeerID) (*RealmRes, error) {
	if !s.acl.CanReadAny(peer) {
		return nil, ErrNotAllowed{Subject: peer, Verb: "LIST"}
	}
	var names []string
	if err := s.realm.ForEach(ctx, func(x string) error {
		names = append(names, x)
		return nil
	}); err != nil {
		return nil, err
	}
	return &RealmRes{List: names}, nil
}

func checkACL(acl ACL, peer p2p.PeerID, name string, write bool) error {
	var err error
	if write {
		verb := "WRITE"
		if !acl.CanWrite(peer, name) {
			err = ErrNotAllowed{
				Subject: peer,
				Verb:    verb,
				Object:  name,
			}
		}
	} else {
		verb := "READ"
		if !acl.CanRead(peer, name) {
			err = ErrNotAllowed{
				Subject: peer,
				Verb:    verb,
				Object:  name,
			}
		}
	}
	return err
}

type RealmReq struct {
	Op   string `json:"op"`
	Name string `json:"name"`
}

type RealmRes struct {
	Error  *string  `json:"error,omitempty"`
	Exists *bool    `json:"exists,omitempty"`
	List   []string `json:"list,omitempty"`
}

var _ branches.Realm = &realm{}

type realm struct {
	srv      *realmSrv
	peer     p2p.PeerID
	newCell  func(CellID) cells.Cell
	newStore func(StoreID) cadata.Store
}

func newRealm(srv *realmSrv, peer p2p.PeerID, newCell func(CellID) cells.Cell, newStore func(StoreID) cadata.Store) *realm {
	return &realm{
		srv:      srv,
		peer:     peer,
		newCell:  newCell,
		newStore: newStore,
	}
}

func (r *realm) Create(ctx context.Context, name string) error {
	return r.srv.Create(ctx, BranchID{Peer: r.peer, Name: name})
}

func (r *realm) Get(ctx context.Context, name string) (*branches.Branch, error) {
	if yes, err := r.srv.Exists(ctx, BranchID{Peer: r.peer, Name: name}); err != nil {
		return nil, err
	} else if !yes {
		return nil, branches.ErrNotExist
	}
	b := branches.Branch{
		Volume: branches.Volume{
			Cell:     r.newCell(CellID{Peer: r.peer, Name: name}),
			VCStore:  r.newStore(StoreID{Peer: r.peer, Branch: name, Type: Type_VC}),
			FSStore:  r.newStore(StoreID{Peer: r.peer, Branch: name, Type: Type_FS}),
			RawStore: r.newStore(StoreID{Peer: r.peer, Branch: name, Type: Type_RAW}),
		},
	}
	return &b, nil
}

func (r *realm) Delete(ctx context.Context, name string) error {
	return r.srv.Delete(ctx, BranchID{Peer: r.peer, Name: name})
}

func (r *realm) ForEach(ctx context.Context, fn func(string) error) error {
	panic("not implemented")
}
