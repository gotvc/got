package gotcmd

import (
	"context"
	"fmt"

	"github.com/gotvc/got/src/internal/gotcore"
	"github.com/gotvc/got/src/internal/gotfsfix"
	"go.brendoncarroll.net/star"
)

var fixNameParam = star.Required[string]{
	ShortDoc: "the name of the fix to apply to the filesystem",
	Parse:    star.ParseString,
}

var fixCmd = star.Command{
	Metadata: star.Metadata{
		Short: "repair filesystems and histories",
	},
	Pos: []star.Positional{fixNameParam, markNameParam, newMarkNameParam},
	F: func(c star.Context) error {
		src := markNameParam.Load(c)
		dst := newMarkNameParam.Load(c)
		fixName := fixNameParam.Load(c)

		r, err := openRepo()
		if err != nil {
			return err
		}
		ctx := c.Context
		defer r.Close()

		// check that the fixname is valid before the transaction
		var fixFn func(ctx context.Context, srcTx, dstTx *gotcore.MarkTx) error
		switch fixName {
		case "fs-dirs":
			fixFn = fixDirs
		default:
			return fmt.Errorf("unknown fix %q", fixName)
		}

		sp, err := r.GetSpace(ctx, "")
		if err != nil {
			return err
		}
		return sp.Do(ctx, true, func(stx gotcore.SpaceTx) error {
			srctx, err := gotcore.NewMarkTx(ctx, stx, src)
			if err != nil {
				return err
			}
			// Create the destination mark; fails if it already exists.
			if _, err := stx.Create(ctx, dst, srctx.Info().AsMetadata()); err != nil {
				return err
			}
			dsttx, err := gotcore.NewMarkTx(ctx, stx, dst)
			if err != nil {
				return err
			}
			return fixFn(ctx, srctx, dsttx)
		})
	},
}

func fixDirs(ctx context.Context, src, dst *gotcore.MarkTx) error {
	var comm gotcore.Commit
	ok, err := src.LoadCommit(ctx, &comm)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("source mark has no commit")
	}

	vcmach := dst.GotVC()
	fsmach := dst.GotFS()
	ss := dst.FSRW()

	mapped, err := vcmach.Map(ctx, dst.VCRW(), comm, func(p gotcore.Payload) (gotcore.Payload, error) {
		fixed, err := gotfsfix.FixDirs(ctx, fsmach, ss[1], p.Snap)
		if err != nil {
			return p, err
		}
		p.Snap = *fixed
		return p, nil
	})
	if err != nil {
		return err
	}

	ref, err := vcmach.PostVertex(ctx, dst.VCRW(), mapped)
	if err != nil {
		return err
	}
	return dst.Save(ctx, ref)
}
