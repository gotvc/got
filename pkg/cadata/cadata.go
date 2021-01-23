package cadata

import "github.com/blobcache/blobcache/pkg/blobs"

type Store = blobs.Store

type ID = blobs.ID

func IDFromBytes(x []byte) ID {
	id := ID{}
	copy(id[:], x)
	return id
}
