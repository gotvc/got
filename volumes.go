package got

import (
	"context"
	"encoding/json"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/cells"
	"github.com/brendoncarroll/got/pkg/gotvc"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

// SyncVolumes moves the commit in src and all it's data from to dst
// if the commit in dst is not an ancestor of src then an error is returned.
// that behavior can be disabled with force=true.
func SyncVolumes(ctx context.Context, dst, src Volume, force bool) error {
	if err := cadata.CopyAll(ctx, dst, src); err != nil {
		return err
	}
	srcSnap, err := getSnapshot(ctx, src.Cell)
	if err != nil {
		return err
	}
	srcRef, err := gotvc.PostSnapshot(ctx, src.Store, *srcSnap)
	if err != nil {
		return err
	}
	return applySnapshot(ctx, dst.Cell, func(x *Commit) (*Commit, error) {
		if x == nil {
			return nil, err
		}
		xRef, err := gotvc.PostSnapshot(ctx, src.Store, *srcSnap)
		if err != nil {
			return nil, err
		}
		hasAncestor, err := gotvc.HasAncestor(ctx, src.Store, *srcRef, *xRef)
		if err != nil {
			return nil, err
		}
		if !force && !hasAncestor {
			return nil, errors.Errorf("cannot CAS, dst ref is not parent of src ref")
		}
		return srcSnap, nil
	})
}

func (r *Repo) CreateVolume(ctx context.Context, name string) error {
	return r.specDir.Create(ctx, name)
}

func (r *Repo) CreateVolumeWithSpec(name string, spec VolumeSpec) error {
	return r.specDir.CreateWithSpec(name, spec)
}

func (r *Repo) DeleteVolume(ctx context.Context, name string) error {
	return r.specDir.Delete(ctx, name)
}

func (r *Repo) GetActiveVolume(ctx context.Context) (string, *Volume, error) {
	name, err := getActiveVolume(r.db)
	if err != nil {
		return "", nil, err
	}
	vol, err := r.GetRealm().Get(ctx, name)
	if err != nil {
		return "", nil, err
	}
	return name, vol, nil
}

func (r *Repo) SetActiveVolume(ctx context.Context, name string) error {
	_, err := r.GetRealm().Get(ctx, name)
	if err != nil {
		return err
	}
	yes, err := r.StagingIsEmpty(ctx)
	if err != nil {
		return err
	}
	if !yes {
		return errors.Errorf("cannot change active volume with non-empty staging")
	}
	return setActiveVolume(r.db, name)
}

func getActiveVolume(db *bolt.DB) (string, error) {
	name := nameMaster
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

func setActiveVolume(db *bolt.DB, name string) error {
	return db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucketDefault))
		if err != nil {
			return err
		}
		return b.Put([]byte(keyActive), []byte(name))
	})
}

func getSnapshot(ctx context.Context, c cells.Cell) (*Commit, error) {
	data, err := c.Get(ctx)
	if err != nil {
		return nil, err
	}
	if len(data) < 0 {
		return nil, nil
	}
	var x Commit
	if err := json.Unmarshal(data, &x); err != nil {
		return nil, err
	}
	return &x, nil
}

func applySnapshot(ctx context.Context, c cells.Cell, fn func(*Commit) (*Commit, error)) error {
	return cells.Apply(ctx, c, func(data []byte) ([]byte, error) {
		var x *Commit
		if len(data) > 0 {
			x = &Commit{}
			if err := json.Unmarshal(data, &x); err != nil {
				return nil, err
			}
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
