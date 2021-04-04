package refs

import (
	"context"
	"hash/crc64"

	"github.com/blobcache/blobcache/pkg/bccrypto"
	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/pkg/errors"
)

const maxNodeSize = blobs.MaxSize

type Store = cadata.Store

func Post(ctx context.Context, s Store, data []byte) (*Ref, error) {
	if len(data) > maxNodeSize {
		return nil, errors.Errorf("data len=%d exceeds max size", len(data))
	}
	kf := bccrypto.SaltedConvergent(nil)
	id, dek, err := bccrypto.Post(ctx, s, kf, data)
	if err != nil {
		return nil, err
	}
	return &Ref{
		CID: id,
		DEK: *dek,
	}, nil
}

func GetF(ctx context.Context, s Store, ref Ref, fn func(data []byte) error) error {
	return bccrypto.GetF(ctx, s, ref.DEK, ref.CID, func(data []byte) error {
		return assertNotModified(data, fn)
	})
}

func assertNotModified(data []byte, fn func(data []byte) error) error {
	before := crc64Sum(data)
	err := fn(data)
	after := crc64Sum(data)
	if before != after {
		panic("buffer modified")
	}
	return err
}

func crc64Sum(data []byte) uint64 {
	return crc64.Checksum(data, crc64.MakeTable(crc64.ISO))
}
