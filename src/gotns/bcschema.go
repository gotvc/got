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
	prevRoot, err := parseRoot(prev)
	if err != nil {
		return err
	}
	nextRoot, err := parseRoot(next)
	if err != nil {
		return err
	}
	mach := New()
	return mach.Validate(ctx, src, prevRoot, nextRoot)
}

func (s Schema) ReadLinks(ctx context.Context, src cadata.Getter, rootData []byte, dst map[blobcache.OID]blobcache.ActionSet) error {
	root, err := parseRoot(rootData)
	if err != nil {
		return err
	}
	if root == nil {
		return nil
	}
	mach := New()
	entries, err := mach.ListEntries(ctx, src, *root, 0)
	if err != nil {
		return err
	}
	for _, ent := range entries {
		dst[ent.Volume] = ent.Rights
	}
	return nil
}
