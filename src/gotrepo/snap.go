package gotrepo

import (
	"context"
	"io"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/marks"
)

type SnapExpr = marks.SnapExpr

// ViewSnapshot calls fn to view a Snapshot in read-only mode
func (r *Repo) ViewSnapshot(ctx context.Context, se SnapExpr, fn func(*marks.ViewCtx) error) error {
	space, err := r.GetSpace(ctx, se.GetSpace())
	if err != nil {
		return err
	}
	return space.Do(ctx, true, func(st marks.SpaceTx) error {
		return marks.ViewSnapshot(ctx, st, se, fn)
	})
}

func (r *Repo) History(ctx context.Context, se SnapExpr, fn func(ref Ref, s Snap) error) error {
	return r.ViewSnapshot(ctx, se, func(vctx *marks.ViewCtx) error {
		return marks.History(ctx, vctx.VC, vctx.Stores[2], *vctx.Root, fn)
	})
}

func (r *Repo) DebugFS(ctx context.Context, se marks.SnapExpr, w io.Writer) error {
	return r.ViewSnapshot(ctx, se, func(vctx *marks.ViewCtx) error {
		return gotfs.Dump(ctx, vctx.Stores[1], vctx.Root.Payload.Root, w)
	})
}

func (r *Repo) DebugKV(ctx context.Context, se marks.SnapExpr, w io.Writer) error {
	return r.ViewSnapshot(ctx, se, func(vctx *marks.ViewCtx) error {
		return gotkv.DebugTree(ctx, vctx.Stores[1], vctx.Root.Payload.Root.ToGotKV(), w)
	})
}
