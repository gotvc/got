package got

import (
	"context"
	"encoding/json"
	"path/filepath"

	"github.com/brendoncarroll/got/pkg/fs"
	bolt "go.etcd.io/bbolt"
)

func (r *Repo) CreateVolume(name string, spec VolumeSpec) error {
	_, err := r.MakeEnv(name, spec)
	if err != nil {
		return err
	}
	p := filepath.Join(specDirPath, name)
	data, err := json.MarshalIndent(spec, "", " ")
	if err != nil {
		return err
	}
	return fs.WriteIfNotExists(r.repoFS, p, data)
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
