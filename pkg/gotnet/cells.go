package gotnet

import (
	"bytes"
	"context"
	"fmt"

	"go.brendoncarroll.net/p2p"
	"go.brendoncarroll.net/state/cells"
	"go.brendoncarroll.net/stdctx/logctx"
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

func (cs *cellSrv) CAS(ctx context.Context, cid CellID, actual *[]byte, prev, next []byte) (bool, error) {
	if len(next) > cellSize {
		return false, cells.ErrTooLarge{}
	}
	req := CellReq{
		CAS: &CASReq{
			Name: cid.Name,
			Prev: prev,
			Next: next,
		},
	}
	reqData := marshal(req)
	// TODO: reuse actual
	resp := make([]byte, cellSize)
	n, err := cs.swarm.Ask(ctx, resp, cid.Peer, p2p.IOVec{reqData})
	if err != nil {
		return false, err
	}
	swapped := bytes.Equal(resp[:n], next)
	cells.CopyBytes(actual, resp[:n])
	return swapped, nil
}

func (cs *cellSrv) Load(ctx context.Context, cid CellID, dst *[]byte) error {
	req := CellReq{
		Read: &ReadReq{
			Name: cid.Name,
		},
	}
	reqData := marshal(req)
	if len(*dst) < cellSize {
		*dst = make([]byte, cellSize)
	}
	n, err := cs.swarm.Ask(ctx, *dst, cid.Peer, p2p.IOVec{reqData})
	if err != nil {
		return err
	}
	*dst = (*dst)[:n]
	return nil
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
			n, err = cs.handleCAS(ctx, msg.Src, req.CAS.Name, resp, req.CAS.Prev, req.CAS.Next)
			if err != nil {
				return err
			}

		default:
			return fmt.Errorf("no request content")
		}
		return nil
	}(); err != nil {
		logctx.Errorf(ctx, "while handling cell request: %v", err)
		return -1
	}
	return n
}

func (cs *cellSrv) handleCAS(ctx context.Context, peer PeerID, name string, actual []byte, prev, next []byte) (int, error) {
	space := cs.open(peer)
	v, err := space.Open(ctx, name)
	if err != nil {
		return 0, err
	}
	cell := v.Cell
	var actual2 []byte
	_, err = cell.CAS(ctx, &actual2, prev, next)
	return copy(actual, actual2), err
}

func (cs *cellSrv) handleRead(ctx context.Context, peer PeerID, name string, buf []byte) (int, error) {
	space := cs.open(peer)
	v, err := space.Open(ctx, name)
	if err != nil {
		return 0, err
	}
	cell := v.Cell
	var buf2 []byte
	if err := cell.Load(ctx, &buf2); err != nil {
		return 0, err
	}
	return copy(buf, buf2), err
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

var _ cells.BytesCell = &cell{}

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

func (c *cell) CAS(ctx context.Context, actual *[]byte, prev, next []byte) (bool, error) {
	return c.srv.CAS(ctx, c.cid, actual, prev, next)
}

func (c *cell) Load(ctx context.Context, dst *[]byte) error {
	return c.srv.Load(ctx, c.cid, dst)
}

func (c *cell) MaxSize() int {
	return cellSize
}

func (c *cell) Copy(dst *[]byte, src []byte) {
	cells.CopyBytes(dst, src)
}

func (c *cell) Equals(a, b []byte) bool {
	return bytes.Equal(a, b)
}
