package gotrepo

import (
	"context"
	"io"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/gotcore"
)

type CommitExpr = gotcore.CommitExpr

// ViewCommit calls fn to view a Commit with a read-only transaction on the space.
func (r *Repo) ViewCommit(ctx context.Context, se CommitExpr, fn func(*gotcore.ViewCtx) error) error {
	space, err := r.GetSpace(ctx, se.GetSpace())
	if err != nil {
		return err
	}
	return space.Do(ctx, false, func(st gotcore.SpaceTx) error {
		return gotcore.ViewCommit(ctx, st, se, fn)
	})
}

func (r *Repo) History(ctx context.Context, se CommitExpr, fn func(ref Ref, s Commit) error) error {
	return r.ViewCommit(ctx, se, func(vctx *gotcore.ViewCtx) error {
		return gotcore.History(ctx, vctx.VC, vctx.Stores.VC, vctx.Target, fn)
	})
}

func (r *Repo) DebugFS(ctx context.Context, se gotcore.CommitExpr, w io.Writer) error {
	return r.ViewCommit(ctx, se, func(vctx *gotcore.ViewCtx) error {
		return gotfs.Dump(ctx, vctx.Stores.FS.Metadata, vctx.Root.Payload.Snap, w)
	})
}

func (r *Repo) DebugKV(ctx context.Context, se gotcore.CommitExpr, w io.Writer) error {
	return r.ViewCommit(ctx, se, func(vctx *gotcore.ViewCtx) error {
		return gotkv.DebugTree(ctx, vctx.Stores.FS.Metadata, vctx.Root.Payload.Snap.ToGotKV(), w)
	})
}
