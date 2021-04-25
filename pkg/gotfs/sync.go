package gotfs

import (
	"bytes"
	"context"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/gdat"
	"github.com/brendoncarroll/got/pkg/gotkv"
)

// Sync ensures dst has all the data reachable from root
// dst and src should both be metadata stores.
// copyData will be called to sync metadata
func Sync(ctx context.Context, dst, src Store, root Root, copyData func(id cadata.ID) error) error {
	return gotkv.Sync(ctx, dst, src, root, func(ent gotkv.Entry) error {
		if isPartKey(ent.Key) {
			part, err := parsePart(ent.Value)
			if err != nil {
				return err
			}
			ref, err := gdat.ParseRef(part.Ref)
			if err != nil {
				return err
			}
			return copyData(ref.CID)
		}
		return nil
	})
}

func isPartKey(x []byte) bool {
	return len(x) >= 9 && bytes.Index(x, []byte{0x00}) == len(x)-9
}
