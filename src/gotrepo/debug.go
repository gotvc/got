package gotrepo

import (
	"context"
	"fmt"
	"io"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv"
)

func (r *Repo) DebugFS(ctx context.Context, w io.Writer) error {
	_, b, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	x, tx, err := b.GetHead(ctx)
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	if x == nil {
		return fmt.Errorf("no snapshot, no root")
	}
	return gotfs.Dump(ctx, tx, x.Payload.Root, w)
}

func (r *Repo) DebugKV(ctx context.Context, w io.Writer) error {
	_, b, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	x, tx, err := b.GetHead(ctx)
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	if x == nil {
		return fmt.Errorf("no snapshot, no root")
	}
	return gotkv.DebugTree(ctx, tx, x.Payload.Root.ToGotKV(), w)
}
