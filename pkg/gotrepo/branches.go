package gotrepo

import (
	"context"

	"github.com/gotvc/got/pkg/gotvc"
	bolt "go.etcd.io/bbolt"
)

// CreateBranch creates a branch using the default spec.
func (r *Repo) CreateBranch(ctx context.Context, name string) (*Branch, error) {
	return r.realm.Create(ctx, name)
}

// CreateBranchWithSpec creates a branch using spec
func (r *Repo) CreateBranchWithSpec(name string, spec BranchSpec) (*Branch, error) {
	return r.specDir.CreateWithSpec(name, spec)
}

// DeleteBranch deletes a branch
func (r *Repo) DeleteBranch(ctx context.Context, name string) error {
	return r.realm.Delete(ctx, name)
}

// GetBranch returns the branch with the specified name
func (r *Repo) GetBranch(ctx context.Context, name string) (*Branch, error) {
	if name == "" {
		_, branch, err := r.GetActiveBranch(ctx)
		return branch, err
	}
	return r.realm.Get(ctx, name)
}

// ForEachBranch calls fn once for each branch, or until an error is returned from fn
func (r *Repo) ForEachBranch(ctx context.Context, fn func(string) error) error {
	return r.realm.ForEach(ctx, fn)
}

// SetActiveBranch sets the active branch to name
func (r *Repo) SetActiveBranch(ctx context.Context, name string) error {
	_, err := r.GetSpace().Get(ctx, name)
	if err != nil {
		return err
	}
	return setActiveBranch(r.db, name)
}

// GetActiveBranch returns the name of the active branch, and the branch
func (r *Repo) GetActiveBranch(ctx context.Context) (string, *Branch, error) {
	name, err := getActiveBranch(r.db)
	if err != nil {
		return "", nil, err
	}
	if name == "" {
		name = nameMaster
	}
	branch, err := r.GetSpace().Get(ctx, name)
	if err != nil {
		return "", nil, err
	}
	return name, branch, nil
}

func (r *Repo) SetBranchHead(ctx context.Context, name string, snap Snap) error {
	branch, err := r.GetBranch(ctx, name)
	if err != nil {
		return err
	}
	return applySnapshot(ctx, branch.Volume.Cell, func(x *Snap) (*Snap, error) {
		if err := syncStores(ctx, tripleFromVolume(branch.Volume), r.stagingTriple(), snap); err != nil {
			return nil, err
		}
		return &snap, nil
	})
}

func (r *Repo) GetBranchHead(ctx context.Context, name string) (*Snap, error) {
	branch, err := r.GetBranch(ctx, name)
	if err != nil {
		return nil, err
	}
	return getSnapshot(ctx, branch.Volume.Cell)
}

// Fork creates a new branch called next and sets its head to match base's
func (r *Repo) Fork(ctx context.Context, base, next string) error {
	baseBranch, err := r.GetBranch(ctx, base)
	if err != nil {
		return err
	}

	nextBranch, err := r.CreateBranch(ctx, next)
	if err != nil {
		return err
	}
	if err := syncVolumes(ctx, nextBranch.Volume, baseBranch.Volume, false); err != nil {
		return err
	}

	return r.SetActiveBranch(ctx, next)
}

func (r *Repo) History(ctx context.Context, name string, fn func(ref Ref, s Snap) error) error {
	var err error
	var branch *Branch
	if name == "" {
		_, branch, err = r.GetActiveBranch(ctx)
		if err != nil {
			return err
		}
	} else {
		branch, err = r.GetBranch(ctx, name)
		if err != nil {
			return err
		}
	}
	vol := branch.Volume
	snap, err := getSnapshot(ctx, vol.Cell)
	if err != nil {
		return err
	}
	if snap == nil {
		return nil
	}
	return gotvc.ForEachAncestor(ctx, vol.VCStore, *snap, fn)
}

func getActiveBranch(db *bolt.DB) (string, error) {
	var name string
	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketDefault))
		if b == nil {
			return nil
		}
		v := b.Get([]byte(keyActive))
		if len(v) > 0 {
			name = string(v)
		}
		return nil
	}); err != nil {
		return "", err
	}
	return name, nil
}

func setActiveBranch(db *bolt.DB, name string) error {
	return db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucketDefault))
		if err != nil {
			return err
		}
		return b.Put([]byte(keyActive), []byte(name))
	})
}
