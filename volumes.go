package got

import (
	"context"
	"encoding/json"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/cells"
	"github.com/brendoncarroll/got/pkg/gotfs"
	"github.com/brendoncarroll/got/pkg/gotkv"
	"github.com/brendoncarroll/got/pkg/gotvc"
	"github.com/brendoncarroll/got/pkg/volumes"
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
	srcRef, err := GetRef(ctx, src.Cell)
	if err != nil {
		return err
	}
	return ApplyRef(ctx, dst.Cell, func(x *Ref) (*Ref, error) {
		if x == nil {
			return nil, err
		}
		hasAncestor, err := gotvc.HasAncestor(ctx, src.Store, *srcRef, *x)
		if err != nil {
			return nil, err
		}
		if !force && !hasAncestor {
			return nil, errors.Errorf("cannot CAS, dst ref is not parent of src ref")
		}
		return srcRef, nil
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

func parseRef(data []byte) (*Ref, error) {
	var ref gotkv.Ref
	if len(data) == 0 {
		return &ref, nil
	}
	if err := json.Unmarshal(data, &ref); err != nil {
		return nil, err
	}
	return &ref, nil
}

func marshalRef(x Ref) ([]byte, error) {
	return json.Marshal(x)
}

func GetRef(ctx context.Context, c cells.Cell) (*Ref, error) {
	data, err := c.Get(ctx)
	if err != nil {
		return nil, err
	}
	if len(data) < 1 {
		return nil, nil
	}
	return parseRef(data)
}

func ApplyRef(ctx context.Context, c cells.Cell, fn func(*Ref) (*Ref, error)) error {
	return cells.Apply(ctx, c, func(x []byte) ([]byte, error) {
		var xRef *Ref
		if len(x) > 0 {
			var err error
			xRef, err = parseRef(x)
			if err != nil {
				return nil, err
			}
		}
		yRef, err := fn(xRef)
		if err != nil {
			return nil, err
		}
		return marshalRef(*yRef)
	})
}

func (r *Repo) getRootFromVolume(ctx context.Context, vol volumes.Volume) (*gotfs.Root, error) {
	snapRef, err := GetRef(ctx, vol.Cell)
	if err != nil {
		return nil, err
	}
	if snapRef != nil {
		// use the root from the active volume
		snap, err := gotvc.GetSnapshot(ctx, vol.Store, *snapRef)
		if err != nil {
			return nil, err
		}
		return &snap.Root, nil
	}
	return nil, nil
}

func (r *Repo) createEmptyRoot(ctx context.Context, s cadata.Store) (*gotfs.Root, error) {
	return r.getFSOp().NewEmpty(ctx, s)
}
