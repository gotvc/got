package got

import (
	"context"
	"encoding/json"
	"path/filepath"

	"github.com/brendoncarroll/got/pkg/fs"
	bolt "go.etcd.io/bbolt"
)

func (r *Repo) CreateEnv(name string, spec EnvSpec) error {
	_, err := r.MakeEnv(name, spec)
	if err != nil {
		return err
	}
	p := filepath.Join(cellSpecPath, name)
	data, err := json.MarshalIndent(spec, "", " ")
	if err != nil {
		return err
	}
	return fs.WriteIfNotExists(r.repoFS, p, data)
}

func (r *Repo) DeleteCell(ctx context.Context, name string) error {
	return r.esd.Delete(ctx, name)
}

func (r *Repo) GetActiveCell(ctx context.Context) (string, Cell, error) {
	name, err := getActiveCell(r.db)
	if err != nil {
		return "", nil, err
	}
	env, err := r.GetRealm().Get(ctx, name)
	if err != nil {
		return "", nil, err
	}
	return name, env.Cell, nil
}

func (r *Repo) SetActiveCell(ctx context.Context, name string) error {
	_, err := r.GetRealm().Get(ctx, name)
	if err != nil {
		return err
	}
	return setActiveCell(r.db, name)
}

func getActiveCell(db *bolt.DB) (string, error) {
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

func setActiveCell(db *bolt.DB, name string) error {
	return db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucketDefault))
		if err != nil {
			return err
		}
		return b.Put([]byte(keyActive), []byte(name))
	})
}
