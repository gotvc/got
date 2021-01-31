package cadata

import "github.com/blobcache/blobcache/pkg/blobs"

type Store = blobs.Store

type ID = blobs.ID

func Hash(data []byte) blobs.ID {
	return blobs.Hash(data)
}

func IDFromBytes(x []byte) ID {
	id := ID{}
	copy(id[:], x)
	return id
}

func NewMem() *blobs.MemStore {
	return blobs.NewMem()
}
