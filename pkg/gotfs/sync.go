package gotfs

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/gotvc/got/pkg/metrics"
)

// Sync ensures dst has all the data reachable from root
// dst and src should both be metadata stores.
// copyData will be called to sync metadata
func Sync(ctx context.Context, srcMeta, srcData, dstMeta, dstData Store, root Root) error {
	rep := metrics.FromContext(ctx)
	defer rep.Begin("syncing gotfs")()
	return gotkv.Sync(ctx, srcMeta, dstMeta, root, func(ent gotkv.Entry) error {
		if isExtentKey(ent.Key) {
			part, err := parseExtent(ent.Value)
			if err != nil {
				return err
			}
			ref, err := gdat.ParseRef(part.Ref)
			if err != nil {
				return err
			}
			return gdat.Copy(ctx, srcData, dstData, ref)
		}
		return nil
	})
}

// Populate adds the ID for all the metadata blobs to mdSet and all the data blobs to dataSet
func Populate(ctx context.Context, s Store, root Root, mdSet, dataSet cadata.Set) error {
	return gotkv.Populate(ctx, s, root, mdSet, func(ent gotkv.Entry) error {
		if isExtentKey(ent.Key) {
			part, err := parseExtent(ent.Value)
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
