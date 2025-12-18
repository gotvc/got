package gotrepo

import (
	"context"
	"fmt"
	"io"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv"
)

func (r *Repo) DebugFS(ctx context.Context, mark FQM, w io.Writer) error {
	b, err := r.GetMark(ctx, mark)
	if err != nil {
		return err
	}
	x, tx, err := b.GetTarget(ctx)
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	if x == nil {
		return fmt.Errorf("no snapshot, no root")
	}
	return gotfs.Dump(ctx, tx, x.Payload.Root, w)
}

func (r *Repo) DebugKV(ctx context.Context, mark FQM, w io.Writer) error {
	b, err := r.GetMark(ctx, mark)
	if err != nil {
		return err
	}
	x, tx, err := b.GetTarget(ctx)
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	if x == nil {
		return fmt.Errorf("no snapshot, no root")
	}
	return gotkv.DebugTree(ctx, tx, x.Payload.Root.ToGotKV(), w)
}
