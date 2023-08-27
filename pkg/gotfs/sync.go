package gotfs

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv"
)

// Sync ensures dst has all the data reachable from root
// dst and src should both be metadata stores.
// copyData will be called to sync metadata
func (a *Agent) Sync(ctx context.Context, srcMeta, srcData, dstMeta, dstData Store, root Root) error {
	return a.gotkv.Sync(ctx, srcMeta, dstMeta, *root.toGotKV(), func(ent gotkv.Entry) error {
		if isExtentKey(ent.Key) {
			ext, err := parseExtent(ent.Value)
			if err != nil {
				return err
			}
			return gdat.Copy(ctx, srcData, dstData, &ext.Ref)
		}
		return nil
	})
}

// Populate adds the ID for all the metadata blobs to mdSet and all the data blobs to dataSet
func (a *Agent) Populate(ctx context.Context, s Store, root Root, mdSet, dataSet cadata.Set) error {
	return a.gotkv.Populate(ctx, s, *root.toGotKV(), mdSet, func(ent gotkv.Entry) error {
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
