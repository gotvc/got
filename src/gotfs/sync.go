package gotfs

import (
	"context"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/state/cadata"
)

// Sync ensures dst has all the data reachable from root
// dst and src should both be metadata stores.
// copyData will be called to sync metadata
func (mach *Machine) Sync(ctx context.Context, src [2]stores.Reading, dst [2]stores.Writing, root Root) error {
	return mach.gotkv.Sync(ctx, src[1], dst[1], *root.toGotKV(), func(ent gotkv.Entry) error {
		if isExtentKey(ent.Key) {
			ext, err := parseExtent(ent.Value)
			if err != nil {
				return err
			}
			return gdat.Copy(ctx, src[0], dst[0], &ext.Ref)
		}
		return nil
	})
}

// Populate adds the ID for all the metadata blobs to mdSet and all the data blobs to dataSet
func (mach *Machine) Populate(ctx context.Context, s stores.Reading, root Root, mdSet, dataSet cadata.Set) error {
	return mach.gotkv.Populate(ctx, s, *root.toGotKV(), mdSet, func(ent gotkv.Entry) error {
		if isExtentKey(ent.Key) {
			ext, err := parseExtent(ent.Value)
			if err != nil {
				return err
			}
			return dataSet.Add(ctx, ext.Ref.CID)
		}
		return nil
	})
}
