package gotrepo

import (
	"bytes"
	"context"
	"fmt"

	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/internal/dbutil"
	"github.com/gotvc/got/src/internal/metrics"
	"github.com/gotvc/got/src/internal/staging"
)

const (
	nameMaster = "master"
)

type BranchInfo = branches.Info

type Branch struct {
	branches.Info
	branches.Volume
}

// CreateBranch creates a branch using the default spec.
func (r *Repo) CreateBranch(ctx context.Context, name string, params branches.Params) (*BranchInfo, error) {
	return r.space.Create(ctx, name, params)
}

// // CreateBranchWithSpec creates a branch using spec
// func (r *Repo) CreateBranchWithSpec(ctx context.Context, name string, spec BranchSpec) (*BranchInfo, error) {
// 	return r.space.CreateWithSpec(ctx, name, spec)
// }

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
	return r.getBranch(ctx, name)
}

// getBranch returns a specific branch, or an error if it does not exist
func (r *Repo) getBranch(ctx context.Context, name string) (*Branch, error) {
	if name == "" {
		return nil, fmt.Errorf("branch name cannot be empty")
	}
	info, err := r.space.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	vol, err := r.space.Open(ctx, name)
	if err != nil {
		return nil, err
	}
	return &Branch{Info: *info, Volume: vol}, nil
}

// SetBranch sets branch metadata
func (r *Repo) SetBranch(ctx context.Context, name string, md branches.Params) error {
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
	desiredBranch, err := r.GetBranch(ctx, name)
	if err != nil {
		return err
	}
	return dbutil.DoTx(ctx, r.db, func(conn *dbutil.Conn) error {
		activeName, err := getActiveBranch(conn)
		if err != nil {
			return err
		}
		activeBranch, err := r.getBranch(ctx, activeName)
		if err != nil {
			return err
		}
		// if active branch has the same salt as the desired branch, then
		// there is no check to do.
		// If they have different salts, then we need to check if the staging area is empty.
		if !bytes.Equal(desiredBranch.Salt, activeBranch.Salt) {
			sa, err := newStagingArea(conn, &activeBranch.Info)
			if err != nil {
				return err
			}
			stage := staging.New(sa)
			isEmpty, err := stage.IsEmpty(ctx)
			if err != nil {
				return err
			}
			if !isEmpty {
				return fmt.Errorf("staging must be empty to change to a branch with a different salt")
			}
		}
		return setActiveBranch(conn, name)
	})
}

// GetActiveBranch returns the name of the active branch, and the branch
func (r *Repo) GetActiveBranch(ctx context.Context) (string, *Branch, error) {
	name, err := dbutil.DoTx1(ctx, r.db, func(conn *dbutil.Conn) (string, error) {
		return getActiveBranch(conn)
	})
	if err != nil {
		return "", nil, err
	}
	info, err := r.GetSpace().Get(ctx, name)
	if err != nil {
		return "", nil, err
	}
	vol, err := r.GetSpace().Open(ctx, name)
	if err != nil {
		return "", nil, err
	}
	return name, &Branch{Info: *info, Volume: vol}, nil
}

// SetBranchHead
func (r *Repo) SetBranchHead(ctx context.Context, name string, snap Snap) error {
	branch, err := r.GetBranch(ctx, name)
	if err != nil {
		return err
	}
	return dbutil.DoTxRO(ctx, r.db, func(conn *dbutil.Conn) error {
		sa, err := newStagingArea(conn, &branch.Info)
		if err != nil {
			return err
		}
		stageTxn, err := r.beginStagingTx(ctx, sa.getSalt(), true)
		if err != nil {
			return err
		}
		defer stageTxn.Abort(ctx)
		return branches.SetHead(ctx, branch.Volume, stageTxn, snap)
	})
}

func (r *Repo) GetBranchHead(ctx context.Context, name string) (*Snap, error) {
	b, err := r.GetBranch(ctx, name)
	if err != nil {
		return nil, err
	}
	snap, tx, err := branches.GetHead(ctx, b.Volume)
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
	baseBranch, err := r.GetBranch(ctx, base)
	if err != nil {
		return err
	}
	_, err = r.CreateBranch(ctx, next, branches.Params{
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
	return branches.History(ctx, branches.NewGotVC(&branch.Info), branch.Volume, fn)
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

func getActiveBranch(conn *dbutil.Conn) (string, error) {
	var ret string
	if err := dbutil.Get(conn, &ret, `SELECT name FROM branches WHERE active > 0 LIMIT 1`); err != nil {
		if dbutil.IsErrNoRows(err) {
			return nameMaster, nil
		}
		return "", err
	}
	return ret, nil
}

func setActiveBranch(conn *dbutil.Conn, name string) error {
	if err := dbutil.Exec(conn, `DELETE FROM branches`); err != nil {
		return err
	}
	if err := dbutil.Exec(conn, `INSERT INTO branches (name, active) VALUES (?, 1)`, name); err != nil {
		return err
	}
	return nil
}
