package gotnet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotauthz"
	"go.brendoncarroll.net/p2p"
	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/stdctx/logctx"
)

type StoreID struct {
	Peer   PeerID
	Branch string
	Type   StoreType
}

type StoreType uint8

const (
	Type_RAW = StoreType(iota)
	Type_FS
	Type_VC
)

type blobPullSrv struct {
	store *tempStore
	swarm p2p.AskSwarm[PeerID]
	open  OpenFunc
}

func newBlobPullSrv(open OpenFunc, ts *tempStore, x p2p.AskSwarm[PeerID]) *blobPullSrv {
	srv := &blobPullSrv{
		store: ts,
		swarm: x,
		open:  open,
	}
	return srv
}

func (srv *blobPullSrv) Serve(ctx context.Context) error {
	return serveAsks(ctx, srv.swarm, srv.handleAsk)
}

// PullFrom asks dst for data that hashes to id
// if the node does not have the data they will send back the hash as a sentinel value.
// if the data sent back has an incorrect hash an error is returned; this is potentially malicious.
// otherwise the data is returned
func (s *blobPullSrv) PullFrom(ctx context.Context, dst PeerID, id cadata.ID, buf []byte) (int, error) {
	n, err := s.swarm.Ask(ctx, buf, dst, p2p.IOVec{id[:]})
	if err != nil {
		return 0, err
	}
	if err := checkPullReply(id, dst, buf[:n]); err != nil {
		return 0, err
	}
	return n, nil
}

func (s *blobPullSrv) handleAsk(ctx context.Context, resp []byte, msg p2p.Message[PeerID]) int {
	peer := msg.Src
	var n int
	if err := func() error {
		if !s.store.IsAllowed(peer) {
			return gotauthz.ErrNotAllowed{Subject: peer, Verb: "PULL"}
		}
		id := cadata.IDFromBytes(msg.Payload)
		var err error
		n, err = s.store.Get(ctx, id, resp)
		if cadata.IsNotFound(err) {
			n = copy(resp, id[:])
			return nil
		} else if err != nil {
			return err
		}
		return nil
	}(); err != nil {
		logctx.Warnf(ctx, "%v", err)
		return -1
	}
	return n
}

func checkPullReply(id cadata.ID, peer PeerID, reply []byte) error {
	if bytes.HasPrefix(reply, id[:]) {
		if len(reply) == len(id) {
			return cadata.ErrNotFound{Key: id}
		} else {
			errdata := reply[len(id):]
			var we WireError
			if err := json.Unmarshal(errdata, &we); err == nil {
				return we
			}
			return fmt.Errorf("unrecognized error %q", errdata)
		}
	}
	if err := cadata.Check(gdat.Hash, id, reply); err != nil {
		return fmt.Errorf("%w from peer %v", err, peer)
	}
	return nil
}

type blobMainSrv struct {
	swarm       p2p.AskSwarm[PeerID]
	blobPullSrv *blobPullSrv
	open        OpenFunc
}

func newBlobMainSrv(open OpenFunc, blobGet *blobPullSrv, swarm p2p.AskSwarm[PeerID]) *blobMainSrv {
	srv := &blobMainSrv{
		blobPullSrv: blobGet,
		swarm:       swarm,
		open:        open,
	}
	return srv
}

func (srv *blobMainSrv) Serve(ctx context.Context) error {
	return serveAsks(ctx, srv.swarm, srv.handleAsk)
}

// PushTo sends a set of IDs to the peer dst.
// The remote peer should pull them all, and then respond to the ask with success or failure for each
func (s *blobMainSrv) PushTo(ctx context.Context, sid StoreID, ids []cadata.ID) error {
	resp, err := s.doToIDs(ctx, sid, opPost, ids)
	if err != nil {
		return err
	}
	if resp.Affected != nil && len(resp.Affected) == len(ids) {
		return nil
	}
	return fmt.Errorf("problem pushing, affected count does not match requested id count")
}

func (s *blobMainSrv) Get(ctx context.Context, sid StoreID, id cadata.ID, buf []byte) (int, error) {
	reqData := marshal(BlobReq{
		Op:        opGet,
		Branch:    sid.Branch,
		StoreType: sid.Type,
		IDs:       []cadata.ID{id},
	})
	n, err := s.swarm.Ask(ctx, buf, sid.Peer, p2p.IOVec{reqData})
	if err != nil {
		return 0, err
	}
	if err := checkPullReply(id, sid.Peer, buf[:n]); err != nil {
		return 0, err
	}
	return n, nil
}

func (s *blobMainSrv) Delete(ctx context.Context, sid StoreID, ids []cadata.ID) error {
	_, err := s.doToIDs(ctx, sid, opDelete, ids)
	if err != nil {
		return err
	}
	return nil
}

func (s *blobMainSrv) Exists(ctx context.Context, sid StoreID, ids []cadata.ID) (bool, error) {
	resp, err := s.doToIDs(ctx, sid, opExists, ids)
	if err != nil {
		return false, err
	}
	exists := true
	for _, yes := range resp.Affected {
		exists = exists && yes
	}
	return exists, nil
}

func (s *blobMainSrv) List(ctx context.Context, sid StoreID, first cadata.ID, ids []cadata.ID) (int, error) {
	var resp BlobResp
	req := BlobReq{
		Op:        opList,
		Branch:    sid.Branch,
		StoreType: sid.Type,
		First:     first,
		Limit:     len(ids),
	}
	if err := askJson(ctx, s.swarm, sid.Peer, &resp, req); err != nil {
		return 0, err
	}
	var err error
	return copy(ids, resp.IDs), err
}

func (s *blobMainSrv) doToIDs(ctx context.Context, sid StoreID, op string, ids []cadata.ID) (*BlobResp, error) {
	var resp BlobResp
	req := BlobReq{
		Op:        op,
		IDs:       ids,
		Branch:    sid.Branch,
		StoreType: sid.Type,
	}
	if err := askJson(ctx, s.swarm, sid.Peer, &resp, req); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (s *blobMainSrv) handleAsk(ctx context.Context, respBuf []byte, msg p2p.Message[PeerID]) int {
	var n int
	if err := func() error {
		var req BlobReq
		if err := unmarshal(msg.Payload, &req); err != nil {
			return err
		}
		peer := msg.Src
		if req.Op == opGet {
			var err error
			n, err = s.handleGet(ctx, peer, req, respBuf)
			return err
		}
		resp, err := func() (*BlobResp, error) {
			switch req.Op {
			case opExists:
				return s.handleExists(ctx, peer, req)
			case opDelete:
				return s.handleDelete(ctx, peer, req)
			case opList:
				return s.handleList(ctx, peer, req)
			case opPost:
				return s.handlePost(ctx, peer, req)
			default:
				return nil, fmt.Errorf("invalid op: %q", req.Op)
			}
		}()
		if err != nil {
			return err
		}
		data := marshal(resp)
		n = copy(respBuf, data)
		return nil
	}(); err != nil {
		logctx.Errorf(ctx, "%v", err)
		return -1
	}
	return n
}

func (s *blobMainSrv) handleGet(ctx context.Context, peer PeerID, req BlobReq, buf []byte) (ret int, retErr error) {
	space := s.open(peer)
	if len(req.IDs) != 1 {
		return 0, fmt.Errorf("must request exactly one blob at a time")
	}
	id := req.IDs[0]
	vol, err := space.Open(ctx, req.Branch)
	if err != nil {
		return 0, err
	}
	store, err := getStoreFromVolume(*vol, req.StoreType)
	if err != nil {
		return 0, err
	}
	return store.Get(ctx, id, buf)
}

func (s *blobMainSrv) handlePost(ctx context.Context, peer PeerID, req BlobReq) (*BlobResp, error) {
	space := s.open(peer)
	vol, err := space.Open(ctx, req.Branch)
	if err != nil {
		return nil, err
	}
	store, err := getStoreFromVolume(*vol, req.StoreType)
	if err != nil {
		return nil, err
	}
	affected := make([]bool, len(req.IDs))
	buf := make([]byte, MaxMessageSize)
	for i, id := range req.IDs {
		if exists, err := store.Exists(ctx, id); err != nil {
			return nil, err
		} else if exists {
			affected[i] = false
			continue
		}
		n, err := s.blobPullSrv.PullFrom(ctx, peer, id, buf)
		if err != nil {
			return nil, err
		}
		if _, err := store.Post(ctx, buf[:n]); err != nil {
			return nil, err
		}
		affected[i] = true
	}
	return &BlobResp{
		Affected: affected,
	}, nil
}

func (s *blobMainSrv) handleExists(ctx context.Context, peer PeerID, req BlobReq) (*BlobResp, error) {
	space := s.open(peer)
	vol, err := space.Open(ctx, req.Branch)
	if err != nil {
		return nil, err
	}
	store, err := getStoreFromVolume(*vol, req.StoreType)
	if err != nil {
		return nil, err
	}
	affected := make([]bool, len(req.IDs))
	for i := range req.IDs {
		exists, err := store.Exists(ctx, req.IDs[i])
		if err != nil {
			return nil, err
		}
		affected[i] = exists
	}
	return &BlobResp{
		Affected: affected,
	}, nil
}

func (s *blobMainSrv) handleDelete(ctx context.Context, peer PeerID, req BlobReq) (*BlobResp, error) {
	space := s.open(peer)
	vol, err := space.Open(ctx, req.Branch)
	if err != nil {
		return nil, err
	}
	store, err := getStoreFromVolume(*vol, req.StoreType)
	if err != nil {
		return nil, err
	}
	affected := make([]bool, len(req.IDs))
	for i, id := range req.IDs {
		if err := store.Delete(ctx, id); err != nil {
			return nil, err
		}
		affected[i] = true
	}
	return &BlobResp{
		Affected: affected,
	}, nil
}

func (s *blobMainSrv) handleList(ctx context.Context, peer PeerID, req BlobReq) (*BlobResp, error) {
	space := s.open(peer)
	vol, err := space.Open(ctx, req.Branch)
	if err != nil {
		return nil, err
	}
	store, err := getStoreFromVolume(*vol, req.StoreType)
	if err != nil {
		return nil, err
	}
	ids := make([]cadata.ID, req.Limit)
	span := cadata.Span{}.WithLowerIncl(req.First)
	n, err := store.List(ctx, span, ids)
	if err != nil {
		return nil, err
	}
	return &BlobResp{
		IDs: ids[:n],
	}, nil
}

func getStoreFromVolume(vol branches.Volume, st StoreType) (cadata.Store, error) {
	var store cadata.Store
	switch st {
	case Type_VC:
		store = vol.VCStore
	case Type_FS:
		store = vol.FSStore
	case Type_RAW:
		store = vol.RawStore
	default:
		return nil, fmt.Errorf("unrecognized store")
	}
	return store, nil
}

type BlobReq struct {
	Op string `json:"op"`

	Branch    string    `json:"branch"`
	StoreType StoreType `json:"store_type"`

	IDs   []cadata.ID `json:"ids,omitempty"`
	First cadata.ID   `json:"first,omitempty"`
	Limit int         `json:"limit,omitempty"`
}

type BlobResp struct {
	Affected []bool      `json:"affected,omitempty"`
	IDs      []cadata.ID `json:"ids,omitempty"`
}

// tempStore provides a holding area for blobs that are about to be pulled
type tempStore struct {
	mu          sync.Mutex
	n           uint64
	blobHandles map[uint64]cadata.ID
	peerHandles map[uint64]PeerID
	blobRCs     map[cadata.ID]uint64
	peerRCs     map[PeerID]uint64

	store *cadata.MemStore
}

func newTempStore() *tempStore {
	return &tempStore{
		blobHandles: make(map[uint64]cadata.ID),
		peerHandles: make(map[uint64]PeerID),
		blobRCs:     make(map[cadata.ID]uint64),
		peerRCs:     make(map[PeerID]uint64),
		store:       cadata.NewMem(gdat.Hash, MaxMessageSize),
	}
}

func (ts *tempStore) Get(ctx context.Context, id cadata.ID, data []byte) (int, error) {
	return ts.store.Get(ctx, id, data)
}

func (ts *tempStore) MaxSize() int {
	return ts.store.MaxSize()
}

func (ts *tempStore) Hold(data []byte, peer PeerID) (cadata.ID, uint64) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	id, err := ts.store.Post(context.TODO(), data)
	if err != nil {
		panic(err)
	}
	x := ts.n
	ts.n++
	ts.blobHandles[x] = id
	ts.blobRCs[id]++
	ts.peerHandles[x] = peer
	ts.peerRCs[peer]++
	return id, x
}

func (ts *tempStore) Release(x uint64) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	id, exists := ts.blobHandles[x]
	if !exists {
		return
	}
	peer, exists := ts.peerHandles[x]
	if !exists {
		panic("blob exists for handle but not peer")
	}
	// blob
	ts.blobRCs[id]--
	if ts.blobRCs[id] == 0 {
		delete(ts.blobRCs, id)
		if err := ts.store.Delete(context.TODO(), id); err != nil {
			panic(err)
		}
	}
	delete(ts.blobHandles, x)
	// peer
	ts.peerRCs[peer]--
	if ts.peerRCs[peer] == 0 {
		delete(ts.peerRCs, peer)
	}
	delete(ts.peerHandles, x)
}

func (ts *tempStore) IsAllowed(x PeerID) bool {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	_, exists := ts.peerRCs[x]
	return exists
}

var _ cadata.Store = &store{}

type store struct {
	sid StoreID

	blobMainSrv *blobMainSrv
	blobPullSrv *blobPullSrv
}

func newStore(blobMainSrv *blobMainSrv, blobPullSrv *blobPullSrv, sid StoreID) *store {
	return &store{
		sid:         sid,
		blobMainSrv: blobMainSrv,
		blobPullSrv: blobPullSrv,
	}
}

func (s *store) Get(ctx context.Context, id cadata.ID, buf []byte) (int, error) {
	return s.blobMainSrv.Get(ctx, s.sid, id, buf)
}

func (s *store) Post(ctx context.Context, data []byte) (cadata.ID, error) {
	if len(data) > s.MaxSize() {
		return cadata.ID{}, cadata.ErrTooLarge
	}
	id, x := s.blobPullSrv.store.Hold(data, s.sid.Peer)
	defer s.blobPullSrv.store.Release(x)
	return id, s.blobMainSrv.PushTo(ctx, s.sid, []cadata.ID{id})
}

func (s *store) Delete(ctx context.Context, id cadata.ID) error {
	return s.blobMainSrv.Delete(ctx, s.sid, []cadata.ID{id})
}

func (s *store) Exists(ctx context.Context, id cadata.ID) (bool, error) {
	return s.blobMainSrv.Exists(ctx, s.sid, []cadata.ID{id})
}

func (s *store) List(ctx context.Context, span cadata.Span, ids []cadata.ID) (int, error) {
	first := cadata.BeginFromSpan(span)
	n, err := s.blobMainSrv.List(ctx, s.sid, first, ids)
	if err != nil {
		return 0, err
	}
	if _, ok := span.UpperBound(); ok {
		for i, id := range ids[:n] {
			if span.Compare(id, func(a, b cadata.ID) int { return bytes.Compare(a[:], b[:]) }) < 0 {
				n = i
				break
			}
		}
	}
	return n, nil
}

func (s *store) Hash(x []byte) cadata.ID {
	return gdat.Hash(x)
}

func (s *store) MaxSize() int {
	return MaxMessageSize
}
