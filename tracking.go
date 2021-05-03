package got

import (
	"context"
	"strings"

	"github.com/brendoncarroll/got/pkg/fs"
	"github.com/brendoncarroll/got/pkg/gotfs"
	bolt "go.etcd.io/bbolt"
)

func (r *Repo) Track(ctx context.Context, p string) error {
	return r.tracker.Track(ctx, p)
}

func (r *Repo) Untrack(ctx context.Context, p string) error {
	return r.tracker.Untrack(ctx, p)
}

func (r *Repo) ForEachTracked(ctx context.Context, fn func(p string, isDelete bool) error) error {
	return r.tracker.ForEach(ctx, func(p string) error {
		_, err := r.workingDir.Stat(p)
		if err != nil && !fs.IsNotExist(err) {
			return err
		}
		return fn(p, fs.IsNotExist(err))
	})
}

func (r *Repo) Clear(ctx context.Context) error {
	return r.tracker.Clear(ctx)
}

type tracker struct {
	db         *bolt.DB
	bucketPath []string
}

func newTracker(db *bolt.DB, bucketPath []string) *tracker {
	if len(bucketPath) < 1 {
		panic(bucketPath)
	}
	return &tracker{
		db:         db,
		bucketPath: bucketPath,
	}
}

func (t *tracker) Track(ctx context.Context, p string) error {
	return t.db.Batch(func(tx *bolt.Tx) error {
		b, err := bucketFromTx(tx, t.bucketPath)
		if err != nil {
			return err
		}
		p = strings.Trim(p, string(gotfs.Sep))
		return b.Put([]byte(p), nil)
	})
}

func (t *tracker) Untrack(ctx context.Context, p string) error {
	return t.db.Batch(func(tx *bolt.Tx) error {
		b, err := bucketFromTx(tx, t.bucketPath)
		if err != nil {
			return err
		}
		p = strings.Trim(p, string(gotfs.Sep))
		return b.Delete([]byte(p))
	})
}

func (t *tracker) ForEach(ctx context.Context, fn func(p string) error) error {
	// TODO: bolt does not support writes during iteration, so we have to read to memory
	var ps []string
	err := t.db.View(func(tx *bolt.Tx) error {
		b, err := t.getBucket(tx)
		if err != nil {
			return err
		}
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			p := string(k)
			ps = append(ps, p)
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, p := range ps {
		if err := fn(p); err != nil {
			return err
		}
	}
	return nil
}

func (t *tracker) Clear(ctx context.Context) error {
	path := t.bucketPath
	return t.db.Batch(func(tx *bolt.Tx) error {
		if len(path) > 1 {
			b, err := bucketFromTx(tx, path[:len(path)-1])
			if err != nil {
				return err
			}
			err = b.DeleteBucket([]byte(path[len(path)-1]))
			if err == bolt.ErrBucketNotFound {
				err = nil
			}
			return err
		}
		return tx.DeleteBucket([]byte(path[0]))
	})
}

func (t *tracker) IsEmpty(ctx context.Context) (bool, error) {
	empty := true
	err := t.ForEach(ctx, func(p string) error { empty = false; return nil })
	return empty, err
}

func (t *tracker) getBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	return bucketFromTx(tx, t.bucketPath)
}

func bucketFromTx(tx *bolt.Tx, path []string) (*bolt.Bucket, error) {
	type bucketer interface {
		Bucket([]byte) *bolt.Bucket
		CreateBucketIfNotExists([]byte) (*bolt.Bucket, error)
	}
	getBucket := func(b bucketer, key string) (*bolt.Bucket, error) {
		if tx.Writable() {
			return b.CreateBucketIfNotExists([]byte(key))
		} else {
			return tx.Bucket([]byte(key)), nil
		}
	}
	b, err := getBucket(tx, path[0])
	if err != nil {
		return nil, err
	}
	if b == nil {
		return b, nil
	}
	path = path[1:]
	for len(path) > 0 {
		b, err = getBucket(b, path[0])
		if err != nil {
			return nil, err
		}
		if b == nil {
			return b, nil
		}
		path = path[1:]
	}
	return b, nil
}
