package gotvc

import (
	"context"
	"encoding/json"
	"io"
	"time"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/cells"
	"github.com/brendoncarroll/got/pkg/gotfs"
	"github.com/brendoncarroll/got/pkg/gotkv"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// Stage allows batching changes before producing a new snapshot
type Stage struct {
	cell   cells.Cell
	ms, ds cadata.Store
	fsop   *gotfs.Operator
}

func NewStage(cell cells.Cell, ms, ds cadata.Store, fsop *gotfs.Operator) *Stage {
	return &Stage{
		cell: cell,
		ms:   ms,
		ds:   ds,
		fsop: fsop,
	}
}

// Add adds a path to the stage.
func (s *Stage) Add(ctx context.Context, p string, r io.Reader) error {
	kvop := gotkv.NewOperator()
	fileRoot, err := s.fsop.CreateFileRoot(ctx, s.ms, s.ds, r)
	if err != nil {
		return err
	}
	return s.apply(ctx, func(x Delta) (*Delta, error) {
		additions, err := s.fsop.Graft(ctx, s.ms, x.Additions, p, *fileRoot)
		if err != nil {
			return nil, err
		}
		deletions, err := kvop.DeleteSpan(ctx, s.ms, x.Deletions, gotkv.PrefixSpan([]byte(p)))
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
	delta, err := NewDeletion(ctx, s.ms, s.fsop, p)
	if err != nil {
		return err
	}
	kvop := gotkv.NewOperator()
	return s.apply(ctx, func(x Delta) (*Delta, error) {
		deletions, err := kvop.Merge(ctx, s.ms, x.Deletions, delta.Deletions)
		if err != nil {
			return nil, err
		}
		additions, err := s.fsop.RemoveAll(ctx, s.ms, x.Additions, p)
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
		additions, err := s.fsop.RemoveAll(ctx, s.ms, x.Additions, p)
		if err != nil {
			return nil, err
		}
		deletions, err := kvop.DeleteSpan(ctx, s.ms, x.Deletions, gotkv.PrefixSpan([]byte(p)))
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
	eg := errgroup.Group{}
	eg.Go(func() error {
		return s.apply(ctx, func(Delta) (*Delta, error) {
			return nil, nil
		})
	})
	// TODO: this is slow right now.
	// eg.Go(func() error {
	// 	return cadata.DeleteAll(ctx, s.ms)
	// })
	// eg.Go(func() error {
	// 	return cadata.DeleteAll(ctx, s.ds)
	// })
	return eg.Wait()
}

func (s *Stage) Delta(ctx context.Context) (*Delta, error) {
	return s.get(ctx)
}

func (s *Stage) Snapshot(ctx context.Context, base *Snapshot, message string, createdAt *time.Time) (*Snapshot, error) {
	delta, err := s.get(ctx)
	if err != nil {
		return nil, err
	}
	snap, err := ApplyDelta(ctx, s.ms, base, *delta)
	if err != nil {
		return nil, err
	}
	snap.Message = message
	snap.CreatedAt = createdAt
	return snap, nil
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
			d, err := NewEmptyDelta(ctx, s.ms)
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
		return NewEmptyDelta(ctx, s.ms)
	}
	return &delta, nil
}
