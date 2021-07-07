package gotnet

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/got/pkg/branches"
	"github.com/brendoncarroll/got/pkg/cells"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const cellSize = 1 << 16

type CellID struct {
	Peer p2p.PeerID
	Name string
}

type cellSrv struct {
	realm branches.Realm
	acl   ACL
	swarm p2p.AskSwarm
}

func newCellSrv(realm branches.Realm, acl ACL, swarm p2p.AskSwarm) *cellSrv {
	cs := &cellSrv{
		realm: realm,
		acl:   acl,
		swarm: swarm,
	}
	go p2p.ServeBoth(cs.swarm, p2p.NoOpTellHandler, cs.handleAsk)
	return cs
}

func (cs *cellSrv) CAS(ctx context.Context, cid CellID, prev, next []byte) ([]byte, error) {
	req := CellReq{
		CAS: &CASReq{
			Name: cid.Name,
			Prev: prev,
			Next: next,
		},
	}
	reqData, err := json.Marshal(req)
	if err != nil {
		panic(err)
	}
	return cs.swarm.Ask(ctx, cid.Peer, p2p.IOVec{reqData})
}

func (cs *cellSrv) Read(ctx context.Context, cid CellID, buf []byte) (int, error) {
	req := CellReq{
		Read: &ReadReq{
			Name: cid.Name,
		},
	}
	reqData, err := json.Marshal(req)
	if err != nil {
		panic(err)
	}
	data, err := cs.swarm.Ask(ctx, cid.Peer, p2p.IOVec{reqData})
	if err != nil {
		return 0, err
	}
	if len(buf) < len(data) {
		return 0, io.ErrShortBuffer
	}
	return copy(buf, data), nil
}

func (cs *cellSrv) handleAsk(ctx context.Context, msg *p2p.Message, w io.Writer) {
	var req CellReq
	if err := func() error {
		if err := json.Unmarshal(msg.Payload, &req); err != nil {
			return err
		}
		switch {
		case req.Read != nil:
			buf := make([]byte, cellSize)
			n, err := cs.handleRead(ctx, msg.Src.(p2p.PeerID), req.Read.Name, buf)
			if err != nil {
				return err
			}
			data := buf[:n]
			if _, err := w.Write(data); err != nil {
				return err
			}

		case req.CAS != nil:
			buf := make([]byte, cellSize)
			n, err := cs.handleCAS(ctx, msg.Src.(p2p.PeerID), req.CAS.Name, buf, req.CAS.Prev[:], req.CAS.Next)
			if err != nil {
				return err
			}
			data := buf[:n]
			if _, err := w.Write(data); err != nil {
				return err
			}
		default:
			return errors.Errorf("no request content")
		}
		return nil
	}(); err != nil {
		logrus.Error(err)
		return
	}
}

func (cs *cellSrv) handleCAS(ctx context.Context, peer p2p.PeerID, name string, actual, prev, next []byte) (int, error) {
	if !cs.acl.CanRead(peer, name) {
		return 0, ErrNotAllowed{
			Subject: peer,
			Verb:    "WRITE",
			Object:  name,
		}
	}
	branch, err := cs.realm.Get(ctx, name)
	if err != nil {
		return 0, err
	}
	cell := branch.Volume.Cell
	_, n, err := cell.CAS(ctx, actual, prev, next)
	return n, err
}

func (cs *cellSrv) handleRead(ctx context.Context, peer p2p.PeerID, name string, buf []byte) (int, error) {
	if !cs.acl.CanRead(peer, name) {
		return 0, ErrNotAllowed{
			Subject: peer,
			Verb:    "READ",
			Object:  name,
		}
	}
	branch, err := cs.realm.Get(ctx, name)
	if err != nil {
		return 0, err
	}
	cell := branch.Volume.Cell
	return cell.Read(ctx, buf)
}

type CellReq struct {
	CAS  *CASReq  `json:"cas,omitempty"`
	Read *ReadReq `json:"read,omitempty"`
}

type CASReq struct {
	Name string `json:"name"`
	Prev []byte `json:"prev"`
	Next []byte `json:"next"`
}

type CASRes struct {
	Actual []byte `json:"actual"`
}

type ReadReq struct {
	Name string `json:"name"`
}

var _ cells.Cell = &cell{}

type cell struct {
	srv *cellSrv
	cid CellID
}

func newCell(srv *cellSrv, cid CellID) *cell {
	return &cell{
		srv: srv,
		cid: cid,
	}
}

func (c *cell) CAS(ctx context.Context, actual, prev, next []byte) (bool, int, error) {
	actual2, err := c.srv.CAS(ctx, c.cid, prev, next)
	if err != nil {
		return false, 0, err
	}
	n := copy(actual, actual2)
	success := bytes.Equal(next, actual2)
	return success, n, nil
}

func (c *cell) Read(ctx context.Context, buf []byte) (int, error) {
	return c.srv.Read(ctx, c.cid, buf)
}

func (c *cell) MaxSize() int {
	return cellSize
}
