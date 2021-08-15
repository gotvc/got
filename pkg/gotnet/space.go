package gotnet

import (
	"context"
	"encoding/json"
	"time"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/cells"
)

type BranchID struct {
	Peer PeerID
	Name string
}

type spaceSrv struct {
	space branches.Space
	acl   ACL
	swarm p2p.AskSwarm
	log   *logrus.Logger
}

func newSpaceSrv(space branches.Space, acl ACL, swarm p2p.AskSwarm) *spaceSrv {
	return &spaceSrv{
		space: space,
		acl:   acl,
		swarm: swarm,
		log:   logrus.StandardLogger(),
	}
}

func (srv *spaceSrv) Serve(ctx context.Context) error {
	return serveAsks(ctx, srv.swarm, srv.handleAsk)
}

func (s *spaceSrv) Create(ctx context.Context, bid BranchID) error {
	req := SpaceReq{
		Op:   opCreate,
		Name: bid.Name,
	}
	var resp SpaceRes
	if err := askJson(ctx, s.swarm, bid.Peer, &resp, &req); err != nil {
		return err
	}
	if resp.Error != nil {
		return errors.New(*resp.Error)
	}
	return nil
}

func (s *spaceSrv) Delete(ctx context.Context, bid BranchID) error {
	req := SpaceReq{
		Op:   opDelete,
		Name: bid.Name,
	}
	var resp SpaceRes
	if err := askJson(ctx, s.swarm, bid.Peer, &resp, &req); err != nil {
		return err
	}
	if resp.Error != nil {
		return errors.New(*resp.Error)
	}
	return nil
}

func (s *spaceSrv) Exists(ctx context.Context, bid BranchID) (bool, error) {
	req := SpaceReq{
		Op:   opExists,
		Name: bid.Name,
	}
	var resp SpaceRes
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

func (s *spaceSrv) List(ctx context.Context, peer PeerID, first string, limit int) ([]string, error) {
	req := SpaceReq{
		Op:    opList,
		Name:  first,
		Limit: limit,
	}
	var resp SpaceRes
	if err := askJson(ctx, s.swarm, peer, &resp, &req); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, errors.New(*resp.Error)
	}
	return resp.Names, nil
}

func (s *spaceSrv) handleAsk(ctx context.Context, resp []byte, msg p2p.Message) int {
	ctx, cf := context.WithTimeout(context.Background(), time.Minute)
	defer cf()
	res, err := func() (*SpaceRes, error) {
		peer := msg.Src.(inet256.Addr)
		if !s.acl.CanReadAny(peer) && !s.acl.CanWriteAny(peer) {
			return nil, ErrNotAllowed{Subject: peer}
		}
		var req SpaceReq
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
			return s.handleList(ctx, peer, req.Name, req.Limit)
		default:
			return nil, errors.Errorf("unrecognized operation %s", req.Op)
		}
	}()
	if err != nil {
		logrus.Error(err)
		errMsg := err.Error()
		res = &SpaceRes{
			Error: &errMsg,
		}
	}
	data, _ := json.Marshal(res)
	return copy(resp, data)
}

func (s *spaceSrv) handleCreate(ctx context.Context, peer PeerID, name string) (*SpaceRes, error) {
	if err := checkACL(s.acl, peer, name, true, opCreate); err != nil {
		return nil, err
	}
	if err := s.space.Create(ctx, name); err != nil {
		return nil, err
	}
	return &SpaceRes{}, nil
}

func (s *spaceSrv) handleDelete(ctx context.Context, peer PeerID, name string) (*SpaceRes, error) {
	if err := checkACL(s.acl, peer, name, true, opDelete); err != nil {
		return nil, err
	}
	if err := s.space.Delete(ctx, name); err != nil {
		return nil, err
	}
	return &SpaceRes{}, nil
}

func (s *spaceSrv) handleExists(ctx context.Context, peer PeerID, name string) (*SpaceRes, error) {
	if err := checkACL(s.acl, peer, name, false, opExists); err != nil {
		return nil, err
	}
	_, err := s.space.Get(ctx, name)
	if err != nil && err != branches.ErrNotExist {
		return nil, err
	}
	exists := err == nil
	return &SpaceRes{
		Exists: &exists,
	}, nil
}

func (s *spaceSrv) handleList(ctx context.Context, peer PeerID, first string, limit int) (*SpaceRes, error) {
	if err := checkACL(s.acl, peer, "", false, opList); err != nil {
		return nil, err
	}
	var names []string
	if err := s.space.ForEach(ctx, func(x string) error {
		if len(names) >= limit {
			return nil
		}
		names = append(names, x)
		return nil
	}); err != nil {
		return nil, err
	}
	return &SpaceRes{Names: names}, nil
}

func checkACL(acl ACL, peer PeerID, name string, write bool, verb string) error {
	var err error
	if write {
		if !acl.CanWrite(peer, name) {
			err = ErrNotAllowed{
				Subject: peer,
				Verb:    verb,
				Object:  name,
			}
		}
	} else {
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

type SpaceReq struct {
	Op    string `json:"op"`
	Name  string `json:"name"`
	Limit int    `json:"limit,omitempty"`
}

type SpaceRes struct {
	Error  *string  `json:"error,omitempty"`
	Exists *bool    `json:"exists,omitempty"`
	Names  []string `json:"list,omitempty"`
}

var _ branches.Space = &space{}

type space struct {
	srv      *spaceSrv
	peer     PeerID
	newCell  func(CellID) cells.Cell
	newStore func(StoreID) cadata.Store
}

func newSpace(srv *spaceSrv, peer PeerID, newCell func(CellID) cells.Cell, newStore func(StoreID) cadata.Store) *space {
	return &space{
		srv:      srv,
		peer:     peer,
		newCell:  newCell,
		newStore: newStore,
	}
}

func (r *space) Create(ctx context.Context, name string) error {
	return r.srv.Create(ctx, BranchID{Peer: r.peer, Name: name})
}

func (r *space) Get(ctx context.Context, name string) (*branches.Branch, error) {
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

func (r *space) Delete(ctx context.Context, name string) error {
	return r.srv.Delete(ctx, BranchID{Peer: r.peer, Name: name})
}

func (r *space) ForEach(ctx context.Context, fn func(string) error) error {
	var first string
	for {
		names, err := r.srv.List(ctx, r.peer, first, 100)
		if err != nil {
			return err
		}
		for _, name := range names {
			if name == first {
				continue
			}
			if err := fn(name); err != nil {
				return err
			}
		}
		if len(names) == 0 {
			break
		}
		first = names[len(names)-1]
	}
	return nil
}
