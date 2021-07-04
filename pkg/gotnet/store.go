package gotnet

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type blobPullSrv struct {
	store cadata.Store
	swarm p2p.AskSwarm
	acl   ACL
}

func newBlobPullSrv(s cadata.Store, acl ACL, x p2p.AskSwarm) *blobPullSrv {
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
	s           cadata.Store
	blobPullSrv *blobPullSrv
	realm       *realm
	acl         ACL
}

func newBlobMainSrv(s cadata.Store, blobGet *blobPullSrv, acl ACL, swarm p2p.AskSwarm) *blobMainSrv {
	return &blobMainSrv{
		s:           s,
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
func (s *blobMainSrv) PushTo(ctx context.Context, dst p2p.PeerID, ids []cadata.ID) error {
	var resp BlobResp
	req := BlobReq{
		Op:  "PUSH",
		IDs: ids,
	}
	if err := askJson(ctx, s.swarm, dst, &resp, req); err != nil {
		return err
	}
	if resp.Pulled != nil && len(resp.Pulled) == len(ids) {
		return nil
	}
	return errors.Errorf("problem pushing")
}

func (s *blobMainSrv) handleAsk(ctx context.Context, msg *p2p.Message, w io.Writer) {
	var req BlobReq
	if err := json.Unmarshal(msg.Payload, &req); err != nil {
		return
	}
	peer := msg.Src.(p2p.PeerID)
	resp, err := func() (*BlobResp, error) {
		switch req.Op {
		case "PUSH":
			if !s.acl.CanWrite(peer, req.Name) {
				return nil, errors.Errorf("ACL error")
			}
			branch, err := s.realm.Get(ctx, req.Name)
			if err != nil {
				return nil, err
			}
			vol := branch.Volume
			for _, id := range req.IDs {
				data, err := s.blobPullSrv.PullFrom(ctx, peer, id)
				if err != nil {
					return nil, err
				}
				if _, err := vol.RawStore.Post(ctx, data); err != nil {
					return nil, err
				}
			}
			return &BlobResp{}, nil
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

type BlobReq struct {
	Op   string      `json:"op"`
	Name string      `json:"name"`
	IDs  []cadata.ID `json:"ids"`
}

type BlobResp struct {
	Pulled []bool `json:"pulled"`
	Exists []bool `json:"exists"`
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

type store struct {
	peer p2p.PeerID
	name string

	blobPullSrv *blobPullSrv
	blobMainSrv *blobMainSrv
}

func (s *store) GetF(ctx context.Context, id cadata.ID, fn func(data []byte) error) error {
	data, err := s.blobPullSrv.PullFrom(ctx, s.peer, id)
	if err != nil {
		return err
	}
	return fn(data)
}

func (s *store) Post(ctx context.Context, data []byte) (cadata.ID, error) {
	id, err := s.blobPullSrv.store.Post(ctx, data)
	if err != nil {
		return cadata.ID{}, err
	}
	return id, s.blobMainSrv.PushTo(ctx, s.peer, []cadata.ID{id})
}
