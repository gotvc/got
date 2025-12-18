package gotrepo

import (
	"context"
	"strings"

	"github.com/gotvc/got/src/internal/metrics"
	"github.com/gotvc/got/src/marks"
)

const (
	nameMaster = "master"
)

type BranchInfo = marks.Info

type Branch = marks.Mark

// FQM represents a fully qualified Mark name.
type FQM struct {
	Space string
	Name  string
}

func ParseFQName(s string) FQM {
	parts := strings.SplitN(s, ":", 2)
	switch len(parts) {
	case 1:
		return FQM{Name: parts[0]}
	case 2:
		return FQM{Space: parts[0], Name: parts[1]}
	default:
		panic(s)
	}
}

// CreateBranch creates a new mark in the repo's local space.
func (r *Repo) CreateMark(ctx context.Context, fqname FQM, params marks.Params) (*BranchInfo, error) {
	if err := marks.CheckName(fqname.Name); err != nil {
		return nil, err
	}
	space, err := r.GetSpace(ctx, fqname.Space)
	if err != nil {
		return nil, err
	}
	return space.Create(ctx, fqname.Name, params)
}

// GetBranch returns a specific branch, or an error if it does not exist
func (r *Repo) GetMark(ctx context.Context, fqname FQM) (*Branch, error) {
	space, err := r.GetSpace(ctx, fqname.Space)
	if err != nil {
		return nil, err
	}
	return space.Open(ctx, fqname.Name)
}

// DeleteBranch deletes a mark
// The target of the mark may be garbage collected if nothing else
// references it.
func (r *Repo) DeleteMark(ctx context.Context, fqname FQM) error {
	space, err := r.GetSpace(ctx, fqname.Space)
	if err != nil {
		return err
	}
	return space.Delete(ctx, fqname.Name)
}

// ConfigureMark adjusts metadata
func (r *Repo) ConfigureMark(ctx context.Context, fqname FQM, md marks.Params) error {
	space, err := r.GetSpace(ctx, fqname.Space)
	if err != nil {
		return err
	}
	return space.Set(ctx, fqname.Name, md)
}

// ForEachBranch calls fn once for each branch, or until an error is returned from fn
func (r *Repo) ForEachMark(ctx context.Context, spaceName string, fn func(string) error) error {
	space, err := r.GetSpace(ctx, spaceName)
	if err != nil {
		return err
	}
	return marks.ForEach(ctx, space, marks.TotalSpan(), fn)
}

func (r *Repo) GetMarkRoot(ctx context.Context, mark FQM) (*Snap, error) {
	b, err := r.GetMark(ctx, mark)
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
	baseBranch, err := r.GetMark(ctx, FQM{Name: base})
	if err != nil {
		return err
	}
	_, err = r.CreateMark(ctx, FQM{Name: next}, marks.Params{
		Salt: baseBranch.Info.Salt,
	})
	if err != nil {
		return err
	}
	nextBranch, err := r.GetMark(ctx, FQM{Name: next})
	if err != nil {
		return err
	}
	ctx, cf := metrics.Child(ctx, "syncing")
	defer cf()
	if err := marks.Sync(ctx, baseBranch, nextBranch, false); err != nil {
		return err
	}
	return nil
}

func (r *Repo) History(ctx context.Context, mark FQM, fn func(ref Ref, s Snap) error) error {
	branch, err := r.GetMark(ctx, mark)
	if err != nil {
		return err
	}
	return branch.History(ctx, fn)
}

func (r *Repo) CleanupMark(ctx context.Context, mark FQM) error {
	b, err := r.GetMark(ctx, mark)
	if err != nil {
		return err
	}
	ctx, cf := metrics.Child(ctx, "cleanup volume")
	defer cf()
	if err := marks.CleanupVolume(ctx, b.Volume); err != nil {
		return err
	}
	return nil
}
