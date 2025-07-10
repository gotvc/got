package gotrepo

import (
	"bytes"
	"context"
	"fmt"

	"go.brendoncarroll.net/state"
	"go.brendoncarroll.net/state/kv"

	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/internal/metrics"
)

type BranchInfo = branches.Info

type Branch struct {
	branches.Info
	branches.Volume
}

// CreateBranch creates a branch using the default spec.
func (r *Repo) CreateBranch(ctx context.Context, name string, params branches.Config) (*BranchInfo, error) {
	return r.space.Create(ctx, name, params)
}

// CreateBranchWithSpec creates a branch using spec
func (r *Repo) CreateBranchWithSpec(ctx context.Context, name string, spec BranchSpec) (*BranchInfo, error) {
	return r.specDir.CreateWithSpec(ctx, name, spec)
}

// DeleteBranch deletes a branch
func (r *Repo) DeleteBranch(ctx context.Context, name string) error {
	return r.space.Delete(ctx, name)
}

// GetBranch returns the branch with the specified name
func (r *Repo) GetBranch(ctx context.Context, name string) (*Branch, error) {
	if name == "" {
		_, branch, err := r.GetActiveBranch(ctx)
		return branch, err
	}
	info, err := r.space.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	vol, err := r.space.Open(ctx, name)
	if err != nil {
		return nil, err
	}
	return &Branch{Info: *info, Volume: *vol}, nil
}

// SetBranch sets branch metadata
func (r *Repo) SetBranch(ctx context.Context, name string, md branches.Config) error {
	if name == "" {
		name2, _, err := r.GetActiveBranch(ctx)
		if err != nil {
			return err
		}
		name = name2
	}
	return r.space.Set(ctx, name, md)
}

// ForEachBranch calls fn once for each branch, or until an error is returned from fn
func (r *Repo) ForEachBranch(ctx context.Context, fn func(string) error) error {
	return branches.ForEach(ctx, r.space, branches.TotalSpan(), fn)
}

// SetActiveBranch sets the active branch to name
func (r *Repo) SetActiveBranch(ctx context.Context, name string) error {
	branch, err := r.GetBranch(ctx, name)
	if err != nil {
		return err
	}
	isEmpty, err := r.stage.IsEmpty(ctx)
	if err != nil {
		return err
	}
	if !isEmpty {
		current, err := r.GetBranch(ctx, "")
		if err != nil {
			return err
		}
		if !bytes.Equal(branch.Salt, current.Salt) {
			return fmt.Errorf("staging must be empty to change to a branch with a different salt")
		}
	}
	return r.setActiveBranch(ctx, name)
}

// GetActiveBranch returns the name of the active branch, and the branch
func (r *Repo) GetActiveBranch(ctx context.Context) (string, *Branch, error) {
	name, err := r.getActiveBranch(ctx)
	if err != nil {
		return "", nil, err
	}
	if name == "" {
		name = nameMaster
	}
	info, err := r.GetSpace().Get(ctx, name)
	if err != nil {
		return "", nil, err
	}
	v, err := r.GetSpace().Open(ctx, name)
	if err != nil {
		return "", nil, err
	}
	return name, &Branch{Info: *info, Volume: *v}, nil
}

// SetBranchHead
func (r *Repo) SetBranchHead(ctx context.Context, name string, snap Snap) error {
	branch, err := r.GetBranch(ctx, name)
	if err != nil {
		return err
	}
	st, err := r.getImportTriple(ctx, &branch.Info)
	if err != nil {
		return err
	}
	return branches.SetHead(ctx, branch.Volume, *st, snap)
}

func (r *Repo) GetBranchHead(ctx context.Context, name string) (*Snap, error) {
	b, err := r.GetBranch(ctx, name)
	if err != nil {
		return nil, err
	}
	return branches.GetHead(ctx, b.Volume)
}

// Fork creates a new branch called next and sets its head to match base's
func (r *Repo) Fork(ctx context.Context, base, next string) error {
	baseBranch, err := r.GetBranch(ctx, base)
	if err != nil {
		return err
	}
	_, err = r.CreateBranch(ctx, next, branches.Config{
		Salt: baseBranch.Salt,
	})
	if err != nil {
		return err
	}
	nextBranch, err := r.GetBranch(ctx, next)
	if err != nil {
		return err
	}
	ctx, cf := metrics.Child(ctx, "syncing")
	defer cf()
	if err := branches.SyncVolumes(ctx, baseBranch.Volume, nextBranch.Volume, false); err != nil {
		return err
	}
	return r.SetActiveBranch(ctx, next)
}

func (r *Repo) History(ctx context.Context, name string, fn func(ref Ref, s Snap) error) error {
	branch, err := r.GetBranch(ctx, name)
	if err != nil {
		return err
	}
	return branches.History(ctx, branch.Volume, r.getVCOp(&branch.Info), fn)
}

func (r *Repo) CleanupBranch(ctx context.Context, name string) error {
	b, err := r.GetBranch(ctx, name)
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

func (r *Repo) getActiveBranch(ctx context.Context) (string, error) {
	s := r.getKVStore(tableDefault)
	v, err := kv.Get(ctx, s, []byte(keyActive))
	if err != nil && !state.IsErrNotFound[[]byte](err) {
		return "", err
	}
	return string(v), nil
}

func (r *Repo) setActiveBranch(ctx context.Context, name string) error {
	s := r.getKVStore(tableDefault)
	return s.Put(ctx, []byte(keyActive), []byte(name))
}
