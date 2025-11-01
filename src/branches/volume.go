package branches

import (
	"context"
	"encoding/json"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/state/cadata"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotvc"
	"github.com/gotvc/got/src/internal/metrics"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/volumes"
)

type Snap = gotvc.Snap

type (
	Volume   = volumes.Volume
	Tx       = volumes.Tx
	TxParams = volumes.TxParams
)

// SyncVolumes syncs the contents of src to dst.
func SyncVolumes(ctx context.Context, srcVol, dstVol Volume, force bool) error {
	return applySnapshot(ctx, dstVol, func(dst stores.RW, x *gotvc.Snapshot) (*gotvc.Snapshot, error) {
		goal, tx, err := getSnapshot(ctx, srcVol)
		if err != nil {
			return nil, err
		}
		defer tx.Abort(ctx)
		switch {
		case goal == nil && x == nil:
			return nil, nil
		case goal == nil && !force:
			return nil, fmt.Errorf("cannot clear volume without force=true")
		case x == nil:
		case goal.Equals(*x):
		default:
			hasAncestor, err := gotvc.IsDescendentOf(ctx, tx, *goal, *x)
			if err != nil {
				return nil, err
			}
			if !force && !hasAncestor {
				return nil, fmt.Errorf("cannot CAS, dst ref is not parent of src ref")
			}
		}
		if err := syncStores(ctx, tx, dst, *goal); err != nil {
			return nil, err
		}
		return goal, nil
	})
}

// getSnapshot opens a read-only transaction on vol and loads the snapshot from it.
// It returns the snapshot and the still-open transaction, which the caller must Abort.
func getSnapshot(ctx context.Context, vol Volume) (*Snap, Tx, error) {
	tx, err := vol.BeginTx(ctx, blobcache.TxParams{})
	if err != nil {
		return nil, nil, err
	}
	var data []byte
	if err := tx.Load(ctx, &data); err != nil {
		tx.Abort(ctx)
		return nil, nil, err
	}
	var ret *Snap
	if len(data) > 0 {
		ret = &Snap{}
		if err := json.Unmarshal(data, ret); err != nil {
			tx.Abort(ctx)
			return nil, nil, err
		}
	}
	return ret, tx, nil
}

func applySnapshot(ctx context.Context, dstVol Volume, fn func(stores.RW, *Snap) (*Snap, error)) error {
	tx, err := dstVol.BeginTx(ctx, blobcache.TxParams{Mutate: true})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	var xData []byte
	if err := tx.Load(ctx, &xData); err != nil {
		return err
	}
	var xSnap *Snap
	if len(xData) > 0 {
		xSnap = &Snap{}
		if err := json.Unmarshal(xData, xSnap); err != nil {
			return err
		}
	}
	ySnap, err := fn(tx, xSnap)
	if err != nil {
		return err
	}
	var yData []byte
	if ySnap != nil {
		// this is a check for dangling references.
		if err := syncStores(ctx, stores.NewMem(), tx, *ySnap); err != nil {
			return err
		}
		yData, err = json.Marshal(*ySnap)
		if err != nil {
			return err
		}
	}
	if err := tx.Save(ctx, yData); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func syncStores(ctx context.Context, src stores.Reading, dst stores.Writing, snap gotvc.Snapshot) (err error) {
	ctx, cf := metrics.Child(ctx, "syncing gotvc")
	defer cf()
	return gotvc.Sync(ctx, src, dst, snap)
}

// Cleanup ensures that there are no unreachable blobs in volume.
func CleanupVolume(ctx context.Context, vol Volume) error {
	start, tx, err := getSnapshot(ctx, vol)
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	keep := &stores.MemSet{}
	if start != nil {
		if err := gotvc.Populate(ctx, tx, *start, keep, func(root gotfs.Root) error {
			fsag := gotfs.NewMachine()
			return fsag.Populate(ctx, tx, root, keep, keep)
		}); err != nil {
			return err
		}
	}
	// TODO: open a new mutating transaction and delete the blobs
	// for i := range keep {
	// 	logctx.Infof(ctx, "keeping %d blobs", keep[i].Count())
	// 	if count, err := filterStore(ctx, ss[i], keep[i]); err != nil {
	// 		return err
	// 	} else {
	// 		logctx.Infof(ctx, "deleted %d blobs", count)
	// 	}
	// }
	return nil
}

func filterStore(ctx context.Context, s cadata.Store, set cadata.Set) (int, error) {
	var count int
	err := cadata.ForEach(ctx, s, cadata.Span{}, func(id blobcache.CID) error {
		exists, err := set.Exists(ctx, id)
		if err != nil {
			return err
		}
		if exists {
			return nil
		}
		count++
		return s.Delete(ctx, id)
	})
	return count, err
}
