package stores

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
)

var _ Reading = Union{}

type Union []Reading

func (u Union) Get(ctx context.Context, cid blobcache.CID, buf []byte) (int, error) {
	var lastErr error
	for _, r := range u {
		n, err := r.Get(ctx, cid, buf)
		if err == nil {
			return n, nil
		}
		if isNotFound(err) {
			continue
		}
		lastErr = err
	}
	if lastErr != nil {
		return 0, lastErr
	}
	return 0, blobcache.ErrNotFound{}
}

func (u Union) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	dst2 := make([]bool, len(cids))
	for _, r := range u {
		if err := r.Exists(ctx, cids, dst2); err != nil {
			return err
		}
		for i := range dst {
			if dst2[i] {
				dst[i] = true
			}
		}
	}
	return nil
}

func (u Union) MaxSize() int {
	ret := 0
	for _, r := range u {
		ret = max(ret, r.MaxSize())
	}
	return ret
}
