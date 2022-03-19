package gotnet

import (
	"bytes"
	"context"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-state/cells"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const cellSize = 1 << 16

type CellID struct {
	Peer PeerID
	Name string
}

type cellSrv struct {
	open  OpenFunc
	swarm p2p.AskSwarm[PeerID]
}

func newCellSrv(open OpenFunc, swarm p2p.AskSwarm[PeerID]) *cellSrv {
	cs := &cellSrv{
		open:  open,
		swarm: swarm,
	}
	return cs
}

func (cs *cellSrv) Serve(ctx context.Context) error {
	return serveAsks(ctx, cs.swarm, cs.handleAsk)
}

func (cs *cellSrv) CAS(ctx context.Context, cid CellID, actual, prev, next []byte) (int, error) {
	if len(next) > cellSize {
		return 0, cells.ErrTooLarge{}
	}
	req := CellReq{
		CAS: &CASReq{
			Name: cid.Name,
			Prev: prev,
			Next: next,
		},
	}
	reqData := marshal(req)
	return cs.swarm.Ask(ctx, actual, cid.Peer, p2p.IOVec{reqData})
}

func (cs *cellSrv) Read(ctx context.Context, cid CellID, buf []byte) (int, error) {
	req := CellReq{
		Read: &ReadReq{
			Name: cid.Name,
		},
	}
	reqData := marshal(req)
	return cs.swarm.Ask(ctx, buf, cid.Peer, p2p.IOVec{reqData})
}

func (cs *cellSrv) handleAsk(ctx context.Context, resp []byte, msg p2p.Message[PeerID]) int {
	var req CellReq
	var n int
	if err := func() error {
		if err := unmarshal(msg.Payload, &req); err != nil {
			return err
		}
		var err error
		switch {
		case req.Read != nil:
			n, err = cs.handleRead(ctx, msg.Src, req.Read.Name, resp)
			if err != nil {
				return err
			}

		case req.CAS != nil:
			n, err = cs.handleCAS(ctx, msg.Src, req.CAS.Name, resp, req.CAS.Prev[:], req.CAS.Next)
			if err != nil {
				return err
			}

		default:
			return errors.Errorf("no request content")
		}
		return nil
	}(); err != nil {
		logrus.Errorf("while handling cell request: %v", err)
		return -1
	}
	return n
}

func (cs *cellSrv) handleCAS(ctx context.Context, peer PeerID, name string, actual, prev, next []byte) (int, error) {
	space := cs.open(peer)
	branch, err := space.Get(ctx, name)
	if err != nil {
		return 0, err
	}
	cell := branch.Volume.Cell
	_, n, err := cell.CAS(ctx, actual, prev, next)
	return n, err
}

func (cs *cellSrv) handleRead(ctx context.Context, peer PeerID, name string, buf []byte) (int, error) {
	space := cs.open(peer)
	branch, err := space.Get(ctx, name)
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
	n, err := c.srv.CAS(ctx, c.cid, actual, prev, next)
	if err != nil {
		return false, 0, err
	}
	success := bytes.Equal(next, actual[:n])
	return success, n, nil
}

func (c *cell) Read(ctx context.Context, buf []byte) (int, error) {
	return c.srv.Read(ctx, c.cid, buf)
}

func (c *cell) MaxSize() int {
	return cellSize
}
