package p2pkv

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/brendoncarroll/go-p2p"
	"golang.org/x/crypto/sha3"
)

type RequestID [16]byte

type Op uint8

const (
	OpGet = Op(iota)
	OpPost
	OpPut
	OpDelete
	OpCAS
)

type Request struct {
	ID         RequestID
	Op         Op
	Collection string
	Body       json.RawMessage
}

type GetRequest struct {
	Key []byte
}
type GetResponse struct {
	Exists bool
	Value  []byte
}

type PostRequest struct {
	Value []byte
}
type PostResponse struct {
	Key []byte
}

type CASRequest struct {
	Key       []byte
	PrevSum   []byte
	NextValue []byte
}
type CASResponse struct {
	Actual []byte
}

type PutRequest struct {
	Key   []byte
	Value []byte
}
type PutResponse struct{}

type DeleteRequest struct {
	Key []byte
}
type DeleteResponse struct{}

type Status uint8

const (
	StatusOK = Status(iota)
)

type Response struct {
	Status  Status
	Success json.RawMessage
	Error   string
}

func (r *Response) SetError(err error) {
	r.Error = err.Error()
}

type Service interface {
	Collection(peerID p2p.PeerID, name string) Collection
}

type Collection interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Post(ctx context.Context, value []byte) ([]byte, error)

	CAS(ctx context.Context, key, prevSum, next []byte) ([]byte, error)
	Put(ctx context.Context, key, value []byte) error
	Delete(ctx context.Context, key []byte) error
}

func PrevMatches(actualValue, prevSum []byte) bool {
	sum := sha3.Sum256(actualValue)
	return bytes.Equal(sum[:], prevSum)
}

type ErrOpNotDefined struct {
	Op Op
}

func (e ErrOpNotDefined) Error() string {
	return fmt.Sprintf("the operation (%v) is not defined", e.Op)
}

type NullCollection struct {
}

func (c NullCollection) Get(ctx context.Context, key []byte) ([]byte, error) {
	return nil, ErrOpNotDefined{Op: OpGet}
}

func (c NullCollection) Post(ctx context.Context, key []byte) ([]byte, error) {
	return nil, ErrOpNotDefined{Op: OpPost}
}

func (c NullCollection) CAS(ctx context.Context, key, prevSum, nextValue []byte) ([]byte, error) {
	return nil, ErrOpNotDefined{Op: OpCAS}
}

func (c NullCollection) Put(ctx context.Context, key, value []byte) error {
	return ErrOpNotDefined{Op: OpPut}
}

func (c NullCollection) Delete(ctx context.Context, key []byte) error {
	return ErrOpNotDefined{Op: OpDelete}
}

func marshal(x interface{}) []byte {
	data, err := json.Marshal(x)
	if err != nil {
		panic(err)
	}
	return data
}

func unmarshal(data []byte, x interface{}) error {
	return json.Unmarshal(data, x)
}
