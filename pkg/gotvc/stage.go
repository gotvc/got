package gotvc

import (
	"context"
	"encoding/json"
	"io"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/cells"
	"github.com/brendoncarroll/got/pkg/gotfs"
	"github.com/brendoncarroll/got/pkg/gotkv"
	"github.com/pkg/errors"
)

// Stage allows batching changes before producing a new snapshot
type Stage struct {
	cell  cells.Cell
	store cadata.Store
	fsop  *gotfs.Operator
}

func NewStage(cell cells.Cell, store cadata.Store, fsop *gotfs.Operator) *Stage {
	return &Stage{
		cell:  cell,
		store: store,
		fsop:  fsop,
	}
}

// Add adds a path to the stage.
func (s *Stage) Add(ctx context.Context, p string, r io.Reader) error {
	delta, err := NewAddition(ctx, s.store, s.fsop, p, r)
	if err != nil {
		return err
	}
	kvop := gotkv.NewOperator()
	return s.apply(ctx, func(x Delta) (*Delta, error) {
		additions, err := kvop.Merge(ctx, s.store, x.Additions, delta.Additions)
		if err != nil {
			return nil, err
		}
		deletions, err := kvop.DeleteSpan(ctx, s.store, x.Deletions, gotkv.PrefixSpan([]byte(p)))
		if err != nil {
			return nil, err
		}
		return &Delta{
			Additions: *additions,
			Deletions: *deletions,
		}, nil
	})
}

// Remove removes a path from the stage.
func (s *Stage) Remove(ctx context.Context, p string) error {
	delta, err := NewDeletion(ctx, s.store, s.fsop, p)
	if err != nil {
		return err
	}
	kvop := gotkv.NewOperator()
	return s.apply(ctx, func(x Delta) (*Delta, error) {
		deletions, err := kvop.Merge(ctx, s.store, x.Deletions, delta.Deletions)
		if err != nil {
			return nil, err
		}
		additions, err := s.fsop.RemoveAll(ctx, s.store, x.Additions, p)
		if err != nil {
			return nil, err
		}
		return &Delta{
			Additions: *additions,
			Deletions: *deletions,
		}, nil
	})
}

func (s *Stage) Unstage(ctx context.Context, p string) error {
	kvop := gotkv.NewOperator()
	return s.apply(ctx, func(x Delta) (*Delta, error) {
		additions, err := s.fsop.RemoveAll(ctx, s.store, x.Additions, p)
		if err != nil {
			return nil, err
		}
		deletions, err := kvop.DeleteSpan(ctx, s.store, x.Deletions, gotkv.PrefixSpan([]byte(p)))
		if err != nil {
			return nil, err
		}
		return &Delta{
			Additions: *additions,
			Deletions: *deletions,
		}, nil
	})
}

func (s *Stage) Clear(ctx context.Context) error {
	return s.apply(ctx, func(Delta) (*Delta, error) {
		return nil, nil
	})
}

func (s *Stage) Delta(ctx context.Context) (*Delta, error) {
	return s.get(ctx)
}

func (s *Stage) Snapshot(ctx context.Context, base *Snapshot) (*Snapshot, error) {
	delta, err := s.get(ctx)
	if err != nil {
		return nil, err
	}
	return ApplyDelta(ctx, s.store, base, *delta)
}

func (s *Stage) apply(ctx context.Context, fn func(delta Delta) (*Delta, error)) error {
	var called bool
	return cells.Apply(ctx, s.cell, func(data []byte) ([]byte, error) {
		if called {
			return nil, errors.Errorf("concurrent modification to stage")
		}
		called = true

		var x Delta
		if len(data) > 0 {
			if err := json.Unmarshal(data, &x); err != nil {
				return nil, err
			}
		} else {
			d, err := NewEmptyDelta(ctx, s.store)
			if err != nil {
				return nil, err
			}
			x = *d
		}
		y, err := fn(x)
		if err != nil {
			return nil, err
		}
		if y == nil {
			return nil, nil
		}
		return json.Marshal(*y)
	})
}

func (s *Stage) get(ctx context.Context) (*Delta, error) {
	data, err := s.cell.Get(ctx)
	if err != nil {
		return nil, err
	}
	var delta Delta
	if len(data) > 0 {
		if err := json.Unmarshal(data, &delta); err != nil {
			return nil, err
		}
	} else {
		return NewEmptyDelta(ctx, s.store)
	}
	return &delta, nil
}