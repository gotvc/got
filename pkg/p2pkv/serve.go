package p2pkv

import (
	"context"
	"io"
	"log"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/s/peerswarm"
	"github.com/pkg/errors"
)

var ops = map[Op]func(context.Context, Collection, []byte, *Response) error{
	OpGet:    serveGet,
	OpPost:   servePost,
	OpCAS:    serveCAS,
	OpDelete: serveDelete,
}

func Serve(ctx context.Context, s peerswarm.AskSwarm, srv Service) error {
	defer s.OnAsk(nil)
	s.OnAsk(func(ctx context.Context, msg *p2p.Message, w io.Writer) {
		err := func() error {
			req := Request{}
			if err := unmarshal(msg.Payload, &req); err != nil {
				return err
			}
			col := srv.Collection(msg.Src.(p2p.PeerID), req.Collection)
			if col == nil {
				return errors.Errorf("collection not found")
			}
			res := Response{}
			fn, ok := ops[req.Op]
			if !ok {
				return errors.Errorf("invalid op")
			}
			if err := fn(ctx, col, req.Body, &res); err != nil {
				return err
			}
			resData := marshal(res)
			_, err := w.Write(resData)
			return err
		}()
		if err != nil {
			log.Println(err)
		}
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	}
}

func serveGet(ctx context.Context, col Collection, body []byte, res *Response) error {
	getReq := GetRequest{}
	if err := unmarshal(body, &getReq); err != nil {
		return err
	}
	v, err := col.Get(ctx, getReq.Key)
	if err != nil {
		res.SetError(err)
		return nil
	}
	getRes := GetResponse{
		Exists: true,
		Value:  v,
	}
	res.Success = marshal(getRes)
	return nil
}

func servePost(ctx context.Context, col Collection, body []byte, res *Response) error {
	postReq := PostRequest{}
	if err := unmarshal(body, &postReq); err != nil {
		return err
	}
	k, err := col.Post(ctx, postReq.Value)
	if err != nil {
		res.SetError(err)
		return nil
	}
	postRes := PostResponse{
		Key: k,
	}
	res.Success = marshal(postRes)
	return nil
}

func serveCAS(ctx context.Context, col Collection, body []byte, res *Response) error {
	casReq := CASRequest{}
	if err := unmarshal(body, &casReq); err != nil {
		return err
	}
	actual, err := col.CAS(ctx, casReq.Key, casReq.PrevSum, casReq.NextValue)
	if err != nil {
		res.SetError(err)
		return nil
	}
	casRes := CASResponse{
		Actual: actual,
	}
	res.Success = marshal(casRes)
	return nil
}

func serveDelete(ctx context.Context, col Collection, body []byte, res *Response) error {
	deleteReq := DeleteRequest{}
	if err := unmarshal(body, &deleteReq); err != nil {
		return err
	}
	if err := col.Delete(ctx, deleteReq.Key); err != nil {
		res.SetError(err)
		return nil
	}
	res.Success = marshal(DeleteResponse{})
	return nil
}
