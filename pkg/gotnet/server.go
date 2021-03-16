package gotnet

import (
	"context"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/p2pkv"
	bolt "go.etcd.io/bbolt"
)

const (
	bucketBlobs = "blobs"
	bucketCells = "cells"
)

var _ p2pkv.Service = &Server{}

type Server struct {
	r  Repo
	db *bolt.DB

	whitelist func(p2p.PeerID) bool
}

type Repo interface {
	GetDefaultStore() cadata.Store
	GetACL() ACL
}

type ACL interface {
	CanCASAny(p2p.PeerID) bool
	CanGetAny(p2p.PeerID) bool
	CanCASCell(id p2p.PeerID, name string) bool
	CanGetCell(id p2p.PeerID, name string) bool
}

func NewServer(r Repo) *Server {
	return &Server{}
}

func (s *Server) Collection(peerID p2p.PeerID, name string) p2pkv.Collection {
	if !s.whitelist(peerID) {
		return nil
	}
	switch name {
	case "blobs":
		return &blobCollection{store: s.r.GetDefaultStore()}
	case "cells":
		return &cellCollection{}
	default:
		return nil
	}
}

type blobCollection struct {
	store cadata.Store
	p2pkv.NullCollection
}

func (s *blobCollection) Get(ctx context.Context, key []byte) ([]byte, error) {
	id := cadata.IDFromBytes(key)
	var value []byte
	err := s.store.GetF(ctx, id, func(data []byte) error {
		value = append([]byte{}, data...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (s *blobCollection) Post(ctx context.Context, value []byte) ([]byte, error) {
	id, err := s.store.Post(ctx, value)
	return id[:], err
}

type cellCollection struct {
	p2pkv.NullCollection
}
