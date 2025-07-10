package gotrepo

import (
	"context"
	"fmt"
	"io"

	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"

	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/gotrepo/repodb"
)

func (r *Repo) DebugDB(ctx context.Context, w io.Writer) error {
	for _, tid := range []repodb.TableID{
		tableDefault,
		tableStaging,
		tablePorter,
		tableImportCaches,
		tableImportStores,
	} {
		s := r.getKVStore(tid)
		fmt.Fprintf(w, "TABLE: %q\n", tid)
		if err := dumpStore(ctx, w, s); err != nil {
			logctx.Error(ctx, "dumping store", zap.Error(err))
		}
	}
	return nil
}

func (r *Repo) DebugFS(ctx context.Context, w io.Writer) error {
	_, b, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	x, err := branches.GetHead(ctx, b.Volume)
	if err != nil {
		return err
	}
	if x == nil {
		return fmt.Errorf("no snapshot, no root")
	}
	return gotfs.Dump(ctx, b.Volume.FSStore, x.Root, w)
}

func (r *Repo) DebugKV(ctx context.Context, w io.Writer) error {
	_, b, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	x, err := branches.GetHead(ctx, b.Volume)
	if err != nil {
		return err
	}
	if x == nil {
		return fmt.Errorf("no snapshot, no root")
	}
	return gotkv.DebugTree(ctx, b.Volume.FSStore, x.Root.ToGotKV(), w)
}
