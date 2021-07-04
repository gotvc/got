package got

import (
	"context"

	"github.com/brendoncarroll/got/pkg/gotvc"
	bolt "go.etcd.io/bbolt"
)

// CreateBranch creates a branch using the default spec.
func (r *Repo) CreateBranch(ctx context.Context, name string) error {
	return r.realm.Create(ctx, name)
}

// CreateBranchWithSpec creates a branch using spec
func (r *Repo) CreateBranchWithSpec(name string, spec BranchSpec) error {
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

func (r *Repo) SetActiveBranch(ctx context.Context, name string) error {
	_, err := r.GetRealm().Get(ctx, name)
	if err != nil {
		return err
	}
	return setActiveBranch(r.db, name)
}

func (r *Repo) GetActiveBranch(ctx context.Context) (string, *Branch, error) {
	name, err := getActiveBranch(r.db)
	if err != nil {
		return "", nil, err
	}
	if name == "" {
		name = nameMaster
	}
	branch, err := r.GetRealm().Get(ctx, name)
	if err != nil {
		return "", nil, err
	}
	return name, branch, nil
}

func (r *Repo) History(ctx context.Context, name string, fn func(ref Ref, s Commit) error) error {
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
