package gotns

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"go.brendoncarroll.net/state/cadata"
)

var (
	_ schema.Schema    = Schema{}
	_ schema.Container = Schema{}
)

// Schema implements a blobcache Schema for Got Namespaces.
type Schema struct{}

func (s Schema) Validate(ctx context.Context, src cadata.Getter, prev, next []byte) error {
	mach := New()
	if len(prev) == 0 {
		nextRoot, err := ParseRoot(next)
		if err != nil {
			return err
		}
		return mach.ValidateState(ctx, src, nextRoot.State)
	}
	prevRoot, err := ParseRoot(prev)
	if err != nil {
		return err
	}
	nextRoot, err := ParseRoot(next)
	if err != nil {
		return err
	}
	return mach.ValidateChange(ctx, src, prevRoot.State, nextRoot.State, nextRoot.Delta)
}

func (s Schema) ReadLinks(ctx context.Context, src cadata.Getter, rootData []byte, dst map[blobcache.OID]blobcache.ActionSet) error {
	if len(rootData) == 0 {
		return nil
	}
	root, err := ParseRoot(rootData)
	if err != nil {
		return err
	}
	mach := New()
	entries, err := mach.ListEntries(ctx, src, root.State, 0)
	if err != nil {
		return err
	}
	for _, ent := range entries {
		dst[ent.Volume] = ent.Rights
	}
	return nil
}

func (s Schema) Open(ctx context.Context, src cadata.Getter, rootData []byte, peer blobcache.PeerID) (blobcache.ActionSet, error) {
	return blobcache.Action_ALL, nil
}
