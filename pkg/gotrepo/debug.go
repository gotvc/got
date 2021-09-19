package gotrepo

import (
	"context"
	"fmt"
	"io"

	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotkv/ptree"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

func (r *Repo) DebugDB(ctx context.Context, w io.Writer) error {
	return r.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketDefault))
		if b != nil {
			fmt.Fprintf(w, "BUCKET: %s\n", bucketDefault)
			dumpBucket(w, b)
		}
		b = tx.Bucket([]byte(bucketCellData))
		if b != nil {
			fmt.Fprintf(w, "BUCKET: %s\n", bucketCellData)
			dumpBucket(w, b)
		}
		return nil
	})
}

func (r *Repo) DebugFS(ctx context.Context, w io.Writer) error {
	_, branch, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	vol := branch.Volume
	x, err := branches.GetHead(ctx, *branch)
	if err != nil {
		return err
	}
	if x == nil {
		return errors.Errorf("no snapshot, no root")
	}
	return gotfs.Dump(ctx, vol.FSStore, x.Root, w)
}

func (r *Repo) DebugKV(ctx context.Context, w io.Writer) error {
	// TODO: actually use the writer
	_, branch, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	vol := branch.Volume
	x, err := branches.GetHead(ctx, *branch)
	if err != nil {
		return err
	}
	if x == nil {
		return errors.Errorf("no snapshot, no root")
	}
	ptree.DebugTree(vol.FSStore, x.Root)
	return nil
}
