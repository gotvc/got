package gotorg

import (
	"context"
	"encoding/json"

	"blobcache.io/blobcache/src/schema"
	"blobcache.io/blobcache/src/schema/statetrace"
)

var (
	_ schema.Schema = Schema{}
)

var _ schema.Constructor = SchemaConstructor

const SchemaName = "got/org"

func SchemaConstructor(params json.RawMessage, mkSchema schema.Factory) (schema.Schema, error) {
	return Schema{}, nil
}

// Schema implements a blobcache Schema for Got Namespaces.
type Schema struct{}

func (s Schema) ValidateChange(ctx context.Context, change schema.Change) error {
	mach := New()
	if len(change.Prev.Cell) == 0 {
		nextRoot, err := statetrace.Parse(change.Next.Cell, ParseRoot)
		if err != nil {
			return err
		}
		return mach.ValidateState(ctx, change.Next.Store, nextRoot.State.Current)
	}
	prevRoot, err := statetrace.Parse(change.Prev.Cell, ParseRoot)
	if err != nil {
		return err
	}
	nextRoot, err := statetrace.Parse(change.Next.Cell, ParseRoot)
	if err != nil {
		return err
	}
	return mach.led.Validate(ctx, change.Prev.Store, prevRoot, nextRoot)
}
