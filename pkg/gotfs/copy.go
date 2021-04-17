package gotfs

import (
	"bytes"
	"context"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/gotkv"
)

func Copy(ctx context.Context, dst, src Store, root Root) error {
	return gotkv.Copy(ctx, dst, src, root, func(ent gotkv.Entry) error {
		if isPartKey(ent.Key) {
			part, err := parsePart(ent.Value)
			if err != nil {
				return err
			}
			return cadata.Copy(ctx, dst, src, part.Ref.CID)
		}
		return nil
	})
}

func isPartKey(x []byte) bool {
	return len(x) >= 9 && bytes.Index(x, []byte{0x00}) == len(x)-9
}
