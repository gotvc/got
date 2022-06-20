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
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			fmt.Fprintf(w, "BUCKET: %q\n", name)
			return dumpBucket(w, b)
		})
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
	return ptree.DebugTree(ctx, vol.FSStore, x.Root, w)
}
