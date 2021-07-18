package gotfs

import (
	"bytes"
	"context"

	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/gotvc/got/pkg/stores"
)

// Sync ensures dst has all the data reachable from root
// dst and src should both be metadata stores.
// copyData will be called to sync metadata
func Sync(ctx context.Context, dst, src Store, root Root, syncData func(ref gdat.Ref) error) error {
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
			return syncData(*ref)
		}
		return nil
	})
}

func isPartKey(x []byte) bool {
	return len(x) >= 9 && bytes.Index(x, []byte{0x00}) == len(x)-9
}

// Populate adds the ID for all the metadata blobs to mdSet and all the data blobs to dataSet
func Populate(ctx context.Context, s Store, root Root, mdSet, dataSet stores.Set) error {
	return gotkv.Populate(ctx, s, root, mdSet, func(ent gotkv.Entry) error {
		if isPartKey(ent.Key) {
			part, err := parsePart(ent.Value)
			if err != nil {
				return err
			}
			ref, err := gdat.ParseRef(part.Ref)
			if err != nil {
				return err
			}
			return dataSet.Add(ctx, ref.CID)
		}
		return nil
	})
}
