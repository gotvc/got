// Package reposchema implements a Blobcache Schema for a Got Repo.
// This is the schema for the root Volume in the internal Blobcache instance.
package reposchema

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"github.com/gotvc/got/src/gotkv"
	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/state/cadata"
)

const (
	SchemaName_GotRepo = "gotrepo"
	SchemaName_GotNS   = "gotns"
)

var (
	_ schema.Schema    = &Schema{}
	_ schema.Container = &Schema{}
)

// Schema implements a Blobcache Schema for a Got Repo.
type Schema struct {
	GotKV gotkv.Machine
}

func NewSchema() *Schema {
	const meanSize = 1 << 14
	const maxSize = 1 << 22
	mach := gotkv.NewMachine(meanSize, maxSize)
	return &Schema{GotKV: mach}
}

func (sch *Schema) Validate(ctx context.Context, s cadata.Getter, prev, next []byte) error {
	var prevRoot, nextRoot gotkv.Root
	if len(prev) > 0 {
		if err := prevRoot.Unmarshal(prev); err != nil {
			return err
		}
	}
	if err := nextRoot.Unmarshal(next); err != nil {
		return err
	}
	return nil
}

func (sch *Schema) ReadLinks(ctx context.Context, s cadata.Getter, rootData []byte, dst map[blobcache.OID]blobcache.ActionSet) error {
	if len(rootData) == 0 {
		return nil
	}
	var root gotkv.Root
	if err := root.Unmarshal(rootData); err != nil {
		return err
	}
	it := sch.GotKV.NewIterator(s, root, gotkv.Span{})
	return streams.ForEach(ctx, it, func(ent gotkv.Entry) error {
		oid := blobcache.OID(ent.Value)
		dst[oid] = blobcache.Action_ALL
		return nil
	})
}

func (sch *Schema) Open(ctx context.Context, s cadata.Getter, root []byte, peer blobcache.PeerID) (blobcache.ActionSet, error) {
	return blobcache.Action_ALL, nil
}
