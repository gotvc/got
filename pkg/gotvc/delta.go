package gotvc

import (
	"context"

	"github.com/brendoncarroll/got/pkg/gotfs"
	"github.com/brendoncarroll/got/pkg/gotkv"
)

type Delta struct {
	Add    gotfs.Root
	Delete gotkv.Root
}

func NewEmptyDelta(ctx context.Context, s Store) (*Delta, error) {
	kvo := gotkv.NewOperator()
	emptyRoot, err := kvo.NewEmpty(ctx, s)
	if err != nil {
		return nil, err
	}
	return &Delta{
		Add:    *emptyRoot,
		Delete: *emptyRoot,
	}, nil
}

func Diff(ctx context.Context, s Store, a, b Snapshot) (*Delta, error) {
	panic("not implemented")
}

func DiffRoots(ctx context.Context, s Store, a, b Root) (*Delta, error) {
	panic("not implemented")
}

func DiffWithNothing(ctx context.Context, s Store, a Snapshot) (*Delta, error) {
	kvo := gotkv.NewOperator()
	emptyRoot, err := kvo.NewEmpty(ctx, s)
	if err != nil {
		return nil, err
	}
	return &Delta{
		Add:    a.Root,
		Delete: *emptyRoot,
	}, nil
}

// ApplyDelta makes the changes in delta to base and returns the result.
func ApplyDelta(ctx context.Context, s Store, base Snapshot, delta Delta) (*Snapshot, error) {
	panic("not implemented")
	var root *Root

	parentRef, err := PostSnapshot(ctx, s, base)
	if err != nil {
		return nil, err
	}
	return &Snapshot{
		N:      base.N + 1,
		Root:   *root,
		Parent: parentRef,
	}, nil
}

func AddDeltas(ctx context.Context, s Store, a, b Delta) (*Delta, error) {
	panic("not implemented")
}
