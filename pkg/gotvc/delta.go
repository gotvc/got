package gotvc

import (
	"context"
	"io"

	"github.com/brendoncarroll/got/pkg/gdat"
	"github.com/brendoncarroll/got/pkg/gotfs"
	"github.com/brendoncarroll/got/pkg/gotkv"
)

type Delta struct {
	Additions gotkv.Root `json:"additions"`
	Deletions gotkv.Root `json:"deletions"`
}

func NewEmptyDelta(ctx context.Context, s Store) (*Delta, error) {
	kvo := gotkv.NewOperator()
	emptyRoot, err := kvo.NewEmpty(ctx, s)
	if err != nil {
		return nil, err
	}
	return &Delta{
		Additions: *emptyRoot,
		Deletions: *emptyRoot,
	}, nil
}

func NewAddition(ctx context.Context, ms, ds Store, fsop *gotfs.Operator, p string, r io.Reader) (*Delta, error) {
	delta, err := NewEmptyDelta(ctx, ms)
	if err != nil {
		return nil, err
	}
	x, err := fsop.CreateFile(ctx, ms, ds, delta.Additions, p, r)
	if err != nil {
		return nil, err
	}
	delta.Additions = *x
	return delta, nil
}

func NewDeletion(ctx context.Context, s Store, fsop *gotfs.Operator, p string) (*Delta, error) {
	delta, err := NewEmptyDelta(ctx, s)
	if err != nil {
		return nil, err
	}
	kvop := gotkv.NewOperator()
	root, err := kvop.Put(ctx, s, delta.Deletions, []byte(p), nil)
	if err != nil {
		return nil, err
	}
	delta.Deletions = *root
	return delta, nil
}

func Diff(ctx context.Context, s Store, a, b Snapshot) (*Delta, error) {
	panic("not implemented")
}

func DiffWithNothing(ctx context.Context, s Store, a Snapshot) (*Delta, error) {
	kvo := gotkv.NewOperator()
	emptyRoot, err := kvo.NewEmpty(ctx, s)
	if err != nil {
		return nil, err
	}
	return &Delta{
		Additions: a.Root,
		Deletions: *emptyRoot,
	}, nil
}

func (d *Delta) ListAdditionPaths(ctx context.Context, s Store) ([]string, error) {
	fsop := gotfs.NewOperator()
	var additions []string
	if err := fsop.Walk(ctx, s, d.Additions, func(p string, md gotfs.Metadata) error {
		additions = append(additions, p)
		return nil
	}); err != nil {
		return nil, err
	}
	return additions, nil
}

func (d *Delta) ListDeletionPaths(ctx context.Context, s Store) ([]string, error) {
	kvop := gotkv.NewOperator()
	var deletions []string
	if err := kvop.ForEach(ctx, s, d.Deletions, gotkv.TotalSpan(), func(ent gotkv.Entry) error {
		deletions = append(deletions, string(ent.Key))
		return nil
	}); err != nil {
		return nil, err
	}
	return deletions, nil
}

// ApplyDelta makes the changes in delta to base and returns the result.
func ApplyDelta(ctx context.Context, s Store, base *Snapshot, delta Delta) (*Snapshot, error) {
	if base == nil {
		return &Snapshot{
			N:      0,
			Root:   delta.Additions,
			Parent: nil,
		}, nil
	}

	kvop := gotkv.NewOperator()
	fsop := gotfs.NewOperator()
	root := &base.Root
	err := kvop.ForEach(ctx, s, delta.Deletions, gotkv.TotalSpan(), func(ent gotkv.Entry) error {
		var err error
		root, err = fsop.RemoveAll(ctx, s, *root, string(ent.Key))
		return err
	})
	if err != nil {
		return nil, err
	}
	root, err = kvop.Merge(ctx, s, base.Root, delta.Additions)
	if err != nil {
		return nil, err
	}

	var parentRef *gdat.Ref
	if base != nil {
		var err error
		if parentRef, err = PostSnapshot(ctx, s, *base); err != nil {
			return nil, err
		}
	}
	return &Snapshot{
		N:      base.N + 1,
		Root:   *root,
		Parent: parentRef,
	}, nil
}
