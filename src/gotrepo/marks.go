package gotrepo

import (
	"context"
	"fmt"

	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/internal/metrics"
)

const (
	nameMaster = "master"
)

type BranchInfo = branches.Info

type Branch = branches.Branch

// CreateBranch creates a new mark in the repo's local space.
func (r *Repo) CreateMark(ctx context.Context, name string, params branches.Params) (*BranchInfo, error) {
	space, err := r.GetSpace(ctx, "")
	if err != nil {
		return nil, err
	}
	return space.Create(ctx, name, params)
}

// GetBranch returns a specific branch, or an error if it does not exist
func (r *Repo) GetMark(ctx context.Context, name string) (*Branch, error) {
	if name == "" {
		return nil, fmt.Errorf("branch name cannot be empty")
	}
	space, err := r.GetSpace(ctx, "")
	if err != nil {
		return nil, err
	}
	return space.Open(ctx, name)
}

// DeleteBranch deletes a mark
// The target of the mark may be garbage collected if nothing else
// references it.
func (r *Repo) DeleteMark(ctx context.Context, name string) error {
	space, err := r.GetSpace(ctx, "")
	if err != nil {
		return err
	}
	return space.Delete(ctx, name)
}

// ConfigureMark adjusts metadata
func (r *Repo) ConfigureMark(ctx context.Context, name string, md branches.Params) error {
	space, err := r.GetSpace(ctx, "")
	if err != nil {
		return err
	}
	return space.Set(ctx, name, md)
}

// ForEachBranch calls fn once for each branch, or until an error is returned from fn
func (r *Repo) ForEachMark(ctx context.Context, spaceName string, fn func(string) error) error {
	space, err := r.GetSpace(ctx, spaceName)
	if err != nil {
		return err
	}
	return branches.ForEach(ctx, space, branches.TotalSpan(), fn)
}

func (r *Repo) GetMarkRoot(ctx context.Context, name string) (*Snap, error) {
	b, err := r.GetMark(ctx, name)
	if err != nil {
		return nil, err
	}
	snap, tx, err := b.GetTarget(ctx)
	if err != nil {
		return nil, err
	}
	if err := tx.Abort(ctx); err != nil {
		return nil, err
	}
	return snap, nil
}

// Fork creates a new branch called next and sets its head to match base's
func (r *Repo) Fork(ctx context.Context, base, next string) error {
	baseBranch, err := r.GetMark(ctx, base)
	if err != nil {
		return err
	}
	_, err = r.CreateMark(ctx, next, branches.Params{
		Salt: baseBranch.Info.Salt,
	})
	if err != nil {
		return err
	}
	nextBranch, err := r.GetMark(ctx, next)
	if err != nil {
		return err
	}
	ctx, cf := metrics.Child(ctx, "syncing")
	defer cf()
	if err := branches.Sync(ctx, baseBranch, nextBranch, false); err != nil {
		return err
	}
	return nil
}

func (r *Repo) History(ctx context.Context, name string, fn func(ref Ref, s Snap) error) error {
	branch, err := r.GetMark(ctx, name)
	if err != nil {
		return err
	}
	return branch.History(ctx, fn)
}

func (r *Repo) CleanupMark(ctx context.Context, name string) error {
	b, err := r.GetMark(ctx, name)
	if err != nil {
		return err
	}
	ctx, cf := metrics.Child(ctx, "cleanup volume")
	defer cf()
	if err := branches.CleanupVolume(ctx, b.Volume); err != nil {
		return err
	}
	return nil
}
