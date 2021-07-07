package gotnet

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/got/pkg/branches"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const maxBlobSize = 1 << 20

type StoreID struct {
	Peer   p2p.PeerID
	Branch string
	Type   StoreType
}

type StoreType uint8

const (
	Type_VC = StoreType(iota)
	Type_FS
	Type_RAW
)

type blobPullSrv struct {
	store *tempStore
	swarm p2p.AskSwarm
	acl   ACL
}

func newBlobPullSrv(store *tempStore, acl ACL, x p2p.AskSwarm) *blobPullSrv {
	srv := &blobPullSrv{
		swarm: x,
	}
	return srv
}

func (s *blobPullSrv) Serve() error {
	return p2p.ServeBoth(s.swarm, p2p.NoOpTellHandler, s.handleAsk)
}

// PullFrom asks dst for data that hashes to id
// if the node does not have the data they will send back the hash as a sentinel value.
// if the data sent back has an incorrect hash an error is returned; this is potentially malicious.
// otherwise the data is returned
func (s *blobPullSrv) PullFrom(ctx context.Context, dst p2p.PeerID, id cadata.ID) ([]byte, error) {
	respData, err := s.swarm.Ask(ctx, dst, p2p.IOVec{id[:]})
	if err != nil {
		return nil, err
	}
	if bytes.Equal(respData, id[:]) {
		return nil, cadata.ErrTooMany
	}
	if cadata.DefaultHash(respData) != id {
		return nil, errors.Errorf("got bad blob from %v", dst)
	}
	return respData, nil
}

func (s *blobPullSrv) handleAsk(ctx context.Context, msg *p2p.Message, w io.Writer) {
	if !s.acl.CanReadAny(msg.Src.(p2p.PeerID)) {
		return
	}
	if err := func() error {
		id := cadata.IDFromBytes(msg.Payload)
		buf := make([]byte, s.store.MaxSize())
		n, err := s.store.Read(ctx, id, buf)
		if cadata.IsNotFound(err) {
			_, err := w.Write(id[:])
			return err
		} else if err != nil {
			return err
		}
		_, err = w.Write(buf[:n])
		return err
	}(); err != nil {
		logrus.Warn(err)
	}
}

type blobMainSrv struct {
	swarm       p2p.AskSwarm
	blobPullSrv *blobPullSrv
	realm       *realm
	acl         ACL
}

func newBlobMainSrv(blobGet *blobPullSrv, acl ACL, swarm p2p.AskSwarm) *blobMainSrv {
	return &blobMainSrv{
		blobPullSrv: blobGet,
		swarm:       swarm,
		acl:         acl,
	}
}

func (s *blobMainSrv) Serve() error {
	return p2p.ServeBoth(s.swarm, p2p.NoOpTellHandler, s.handleAsk)
}

// PushTo sends a set of IDs to the peer dst.
// The remote peer should pull them all, and then respond to the ask with success or failure for each
func (s *blobMainSrv) PushTo(ctx context.Context, sid StoreID, ids []cadata.ID) error {
	resp, err := s.doToIDs(ctx, sid, opPush, ids)
	if err != nil {
		return err
	}
	if resp.Affected != nil && len(resp.Affected) == len(ids) {
		return nil
	}
	return errors.Errorf("problem pushing")
}

func (s *blobMainSrv) Delete(ctx context.Context, sid StoreID, ids []cadata.ID) error {
	_, err := s.doToIDs(ctx, sid, opDelete, ids)
	if err != nil {
		return err
	}
	return err
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

func (s *blobMainSrv) List(ctx context.Context, sid StoreID, prefix []byte, ids []cadata.ID) (int, error) {
	var resp BlobResp
	req := BlobReq{
		Op:        opList,
		Branch:    sid.Branch,
		StoreType: sid.Type,
	}
	if err := askJson(ctx, s.swarm, sid.Peer, &resp, req); err != nil {
		return 0, err
	}
	return copy(ids, resp.IDs), nil
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

func (s *blobMainSrv) handleAsk(ctx context.Context, msg *p2p.Message, w io.Writer) {
	var req BlobReq
	if err := json.Unmarshal(msg.Payload, &req); err != nil {
		return
	}
	peer := msg.Src.(p2p.PeerID)
	resp, err := func() (*BlobResp, error) {
		switch req.Op {
		case opPush:
			return s.handlePush(ctx, peer, req)
		default:
			return nil, errors.Errorf("invalid op: %q", req.Op)
		}
	}()
	if err != nil {
		logrus.Error(err)
		return
	}
	data, err := json.Marshal(resp)
	if err != nil {
		logrus.Error(err)
		return
	}
	w.Write(data)
}

func (s *blobMainSrv) handlePush(ctx context.Context, peer p2p.PeerID, req BlobReq) (*BlobResp, error) {
	if !s.acl.CanWrite(peer, req.Branch) {
		return nil, ErrNotAllowed{
			Subject: peer,
			Verb:    "WRITE",
			Object:  req.Branch,
		}
	}
	branch, err := s.realm.Get(ctx, req.Branch)
	if err != nil {
		return nil, err
	}
	vol := branch.Volume
	store, err := getStoreFromVolume(vol, req.StoreType)
	if err != nil {
		return nil, err
	}
	for _, id := range req.IDs {
		data, err := s.blobPullSrv.PullFrom(ctx, peer, id)
		if err != nil {
			return nil, err
		}
		if _, err := store.Post(ctx, data); err != nil {
			return nil, err
		}
	}
	return &BlobResp{}, nil
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
		return nil, errors.Errorf("unrecognized store")
	}
	return store, nil
}

type BlobReq struct {
	Op string `json:"op"`

	Branch    string    `json:"branch"`
	StoreType StoreType `json:"store_type"`

	IDs    []cadata.ID `json:"ids,omitempty"`
	Prefix []byte      `json:"prefix,omitempty"`
}

type BlobResp struct {
	Affected []bool      `json:"affected,omitempty"`
	IDs      []cadata.ID `json:"ids,omitempty"`
}

// tempStore provides a holding area for blobs that are about to be pulled
type tempStore struct {
	mu      sync.Mutex
	n       uint64
	handles map[uint64]cadata.ID
	rcs     map[cadata.ID]uint64

	store *cadata.MemStore
}

func newTempStore() *tempStore {
	return &tempStore{
		handles: make(map[uint64]cadata.ID),
		rcs:     make(map[cadata.ID]uint64),
		store:   cadata.NewMem(maxBlobSize),
	}
}

func (ts *tempStore) Read(ctx context.Context, id cadata.ID, data []byte) (int, error) {
	return ts.store.Read(ctx, id, data)
}

func (ts *tempStore) MaxSize() int {
	return ts.store.MaxSize()
}

func (ts *tempStore) Hold(data []byte) (cadata.ID, uint64) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	id, err := ts.store.Post(context.TODO(), data)
	if err != nil {
		panic(err)
	}
	x := ts.n
	ts.n++
	ts.handles[x] = id
	ts.rcs[id]++
	return id, x
}

func (ts *tempStore) Release(x uint64) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	id, exists := ts.handles[x]
	if !exists {
		return
	}
	ts.rcs[id]--
	if ts.rcs[id] == 0 {
		delete(ts.rcs, id)
	}
	delete(ts.handles, x)
	if err := ts.store.Delete(context.Background(), id); err != nil {
		panic(err)
	}
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

func (s *store) Read(ctx context.Context, id cadata.ID, buf []byte) (int, error) {
	data, err := s.blobPullSrv.PullFrom(ctx, s.sid.Peer, id)
	if err != nil {
		return 0, err
	}
	if len(buf) < len(data) {
		return 0, io.ErrShortBuffer
	}
	n := copy(buf, data)
	return n, nil
}

func (s *store) Post(ctx context.Context, data []byte) (cadata.ID, error) {
	id, x := s.blobPullSrv.store.Hold(data)
	defer s.blobPullSrv.store.Release(x)
	return id, s.blobMainSrv.PushTo(ctx, s.sid, []cadata.ID{id})
}

func (s *store) Delete(ctx context.Context, id cadata.ID) error {
	return s.blobMainSrv.Delete(ctx, s.sid, []cadata.ID{id})
}

func (s *store) Exists(ctx context.Context, id cadata.ID) (bool, error) {
	return s.blobMainSrv.Exists(ctx, s.sid, []cadata.ID{id})
}

func (s *store) List(ctx context.Context, prefix []byte, ids []cadata.ID) (int, error) {
	return s.blobMainSrv.List(ctx, s.sid, prefix, ids)
}

func (s *store) Hash(x []byte) cadata.ID {
	return cadata.DefaultHash(x)
}

func (s *store) MaxSize() int {
	return maxBlobSize
}
