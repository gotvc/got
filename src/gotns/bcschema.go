package gotns

import (
	"context"
	"encoding/json"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
)

var (
	_ schema.Schema = Schema{}
	_ schema.Opener = Schema{}
)

var _ schema.Constructor = SchemaConstructor

func SchemaConstructor(params json.RawMessage, mkSchema schema.Factory) (schema.Schema, error) {
	return Schema{}, nil
}

// Schema implements a blobcache Schema for Got Namespaces.
type Schema struct{}

func (s Schema) ValidateChange(ctx context.Context, change schema.Change) error {
	mach := New()
	if len(change.PrevCell) == 0 {
		nextRoot, err := ParseRoot(change.NextCell)
		if err != nil {
			return err
		}
		return mach.ValidateState(ctx, change.NextStore, nextRoot.State)
	}
	prevRoot, err := ParseRoot(change.PrevCell)
	if err != nil {
		return err
	}
	nextRoot, err := ParseRoot(change.NextCell)
	if err != nil {
		return err
	}
	return mach.led.Validate(ctx, change.PrevStore, prevRoot, nextRoot)
}

func (s Schema) OpenAs(ctx context.Context, src schema.RO, rootData []byte, peer blobcache.PeerID) (blobcache.ActionSet, error) {
	// TODO: restrict permission based on read/write access.
	return blobcache.Action_ALL, nil
}
