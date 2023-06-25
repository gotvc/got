package gotnet

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"errors"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-state/cadata"

	"github.com/brendoncarroll/stdctx/logctx"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/cells"
)

type BranchID struct {
	Peer PeerID
	Name string
}

type spaceSrv struct {
	open  OpenFunc
	swarm p2p.AskSwarm[PeerID]
}

func newSpaceSrv(open OpenFunc, swarm p2p.AskSwarm[PeerID]) *spaceSrv {
	return &spaceSrv{
		open:  open,
		swarm: swarm,
	}
}

func (srv *spaceSrv) Serve(ctx context.Context) error {
	return serveAsks(ctx, srv.swarm, srv.handleAsk)
}

func (s *spaceSrv) Create(ctx context.Context, bid BranchID, cfg branches.Config) (*branches.Info, error) {
	req := SpaceReq{
		Op:     opCreate,
		Name:   bid.Name,
		Config: cfg,
	}
	var resp SpaceRes
	if err := askJson(ctx, s.swarm, bid.Peer, &resp, &req); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, parseWireError(*resp.Error)
	}
	if resp.Info == nil {
		return nil, errors.New("empty branch info with nil error")
	}
	return resp.Info, nil
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
		return parseWireError(*resp.Error)
	}
	return nil
}

func (s *spaceSrv) Set(ctx context.Context, bid BranchID, md branches.Config) error {
	return errors.New("gotnet: setting branch metadata not yet supported ")
}

func (s *spaceSrv) Get(ctx context.Context, bid BranchID) (*branches.Info, error) {
	req := SpaceReq{
		Op:   opGet,
		Name: bid.Name,
	}
	var resp SpaceRes
	if err := askJson(ctx, s.swarm, bid.Peer, &resp, &req); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, parseWireError(*resp.Error)
	}
	if resp.Info == nil {
		return nil, errors.New("empty branch info with nil error")
	}
	return resp.Info, nil
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
		return false, parseWireError(*resp.Error)
	}
	if resp.Exists == nil {
		return false, fmt.Errorf("empty response")
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
		return nil, parseWireError(*resp.Error)
	}
	if !sort.StringsAreSorted(resp.Names) {
		return nil, fmt.Errorf("branch names are unsorted")
	}
	if len(resp.Names) > 0 && resp.Names[0] < first {
		return nil, fmt.Errorf("bad branch listing: %s < %s", resp.Names[0], first)
	}
	return resp.Names, nil
}

func (s *spaceSrv) handleAsk(ctx context.Context, resp []byte, msg p2p.Message[PeerID]) int {
	ctx, cf := context.WithTimeout(ctx, time.Minute)
	defer cf()
	res, err := func() (*SpaceRes, error) {
		peer := msg.Src
		var req SpaceReq
		if err := json.Unmarshal(msg.Payload, &req); err != nil {
			return nil, err
		}
		logctx.Infof(ctx, "%s from %v", req.Op, peer)
		switch req.Op {
		case opCreate:
			return s.handleCreate(ctx, peer, req.Name, req.Config)
		case opDelete:
			return s.handleDelete(ctx, peer, req.Name)
		case opGet:
			return s.handleGet(ctx, peer, req.Name)
		case opExists:
			return s.handleExists(ctx, peer, req.Name)
		case opList:
			return s.handleList(ctx, peer, req.Name, req.Limit)
		default:
			return nil, fmt.Errorf("unrecognized operation %s", req.Op)
		}
	}()
	if err != nil {
		logctx.Errorf(ctx, "%v", err)
		res = &SpaceRes{
			Error: makeWireError(err),
		}
	}
	data, _ := json.Marshal(res)
	return copy(resp, data)
}

func (s *spaceSrv) handleCreate(ctx context.Context, peer PeerID, name string, params branches.Config) (*SpaceRes, error) {
	space := s.open(peer)
	info, err := space.Create(ctx, name, params)
	if err != nil {
		return nil, err
	}
	return &SpaceRes{
		Info: info,
	}, nil
}

func (s *spaceSrv) handleDelete(ctx context.Context, peer PeerID, name string) (*SpaceRes, error) {
	space := s.open(peer)
	if err := space.Delete(ctx, name); err != nil {
		return nil, err
	}
	return &SpaceRes{}, nil
}

func (s *spaceSrv) handleGet(ctx context.Context, peer PeerID, name string) (*SpaceRes, error) {
	space := s.open(peer)
	info, err := space.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	return &SpaceRes{
		Info: info,
	}, nil
}

func (s *spaceSrv) handleExists(ctx context.Context, peer PeerID, name string) (*SpaceRes, error) {
	space := s.open(peer)
	_, err := space.Get(ctx, name)
	if err != nil && err != branches.ErrNotExist {
		return nil, err
	}
	exists := err == nil
	return &SpaceRes{
		Exists: &exists,
	}, nil
}

func (s *spaceSrv) handleList(ctx context.Context, peer PeerID, first string, limit int) (*SpaceRes, error) {
	space := s.open(peer)
	names, err := space.List(ctx, branches.Span{Begin: first}, limit)
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return &SpaceRes{Names: names}, nil
}

type SpaceReq struct {
	Op     string          `json:"op"`
	Name   string          `json:"name"`
	Limit  int             `json:"limit,omitempty"`
	Config branches.Config `json:"config,omitempty"`
}

type SpaceRes struct {
	Error  *WireError     `json:"error,omitempty"`
	Exists *bool          `json:"exists,omitempty"`
	Names  []string       `json:"list,omitempty"`
	Info   *branches.Info `json:"info,omitempty"`
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

func (r *space) Create(ctx context.Context, name string, cfg branches.Config) (*branches.Info, error) {
	return r.srv.Create(ctx, BranchID{Peer: r.peer, Name: name}, cfg)
}

func (r *space) Get(ctx context.Context, name string) (*branches.Info, error) {
	return r.srv.Get(ctx, BranchID{Peer: r.peer, Name: name})
}

func (r *space) Set(ctx context.Context, name string, md branches.Config) error {
	return r.srv.Set(ctx, BranchID{Peer: r.peer, Name: name}, md)
}

func (r *space) Open(ctx context.Context, name string) (*branches.Volume, error) {
	return &branches.Volume{
		Cell:     r.newCell(CellID{Peer: r.peer, Name: name}),
		VCStore:  r.newStore(StoreID{Peer: r.peer, Branch: name, Type: Type_VC}),
		FSStore:  r.newStore(StoreID{Peer: r.peer, Branch: name, Type: Type_FS}),
		RawStore: r.newStore(StoreID{Peer: r.peer, Branch: name, Type: Type_RAW}),
	}, nil
}

func (r *space) Delete(ctx context.Context, name string) error {
	return r.srv.Delete(ctx, BranchID{Peer: r.peer, Name: name})
}

func (r *space) List(ctx context.Context, span branches.Span, limit int) ([]string, error) {
	names, err := r.srv.List(ctx, r.peer, span.Begin, limit)
	if err != nil {
		return nil, err
	}
	for i, name := range names {
		if span.End != "" && name >= span.End {
			names = names[:i]
			break
		}
	}
	return names, nil
}
