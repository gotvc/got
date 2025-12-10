// Package reposchema implements a Blobcache Schema for a Got Repo.
// This is the schema for the root Volume in the internal Blobcache instance.
package reposchema

import (
	"context"
	"encoding/json"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"github.com/gotvc/got/src/gotkv"
)

const (
	SchemaName_GotRepo = "gotrepo"
	SchemeName_GotOrg  = "gotorg"
)

var (
	_ schema.Schema = &Schema{}
	_ schema.Opener = &Schema{}
)

// Schema implements a Blobcache Schema for a Got Repo.
type Schema struct {
	GotKV gotkv.Machine
}

var _ schema.Constructor = Constructor

func Constructor(params json.RawMessage, mkSchema schema.Factory) (schema.Schema, error) {
	return NewSchema(), nil
}

func NewSchema() *Schema {
	const meanSize = 1 << 14
	const maxSize = 1 << 22
	mach := gotkv.NewMachine(meanSize, maxSize)
	return &Schema{GotKV: mach}
}

func (sch *Schema) ValidateChange(ctx context.Context, change schema.Change) error {
	var prevRoot, nextRoot gotkv.Root
	if len(change.PrevCell) > 0 {
		if err := prevRoot.Unmarshal(change.PrevCell); err != nil {
			return err
		}
	}
	if err := nextRoot.Unmarshal(change.NextCell); err != nil {
		return err
	}
	return nil
}

func (sch *Schema) OpenAs(ctx context.Context, s schema.RO, root []byte, peer blobcache.PeerID) (blobcache.ActionSet, error) {
	return blobcache.Action_ALL, nil
}
