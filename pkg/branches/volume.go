package branches

import (
	"context"
	"encoding/json"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/cells"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotvc"
	"github.com/gotvc/got/pkg/stores"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Snap = gotvc.Snap

// Volume is a Cell and a set of stores
type Volume struct {
	RawStore, FSStore, VCStore cadata.Store
	Cell                       cells.Cell
}

func (v Volume) StoreTriple() StoreTriple {
	return StoreTriple{
		Raw: v.RawStore,
		FS:  v.FSStore,
		VC:  v.VCStore,
	}
}

func SyncVolumes(ctx context.Context, src, dst Volume, force bool) error {
	return applySnapshot(ctx, dst.Cell, func(x *gotvc.Snapshot) (*gotvc.Snapshot, error) {
		goal, err := getSnapshot(ctx, src.Cell)
		if err != nil {
			return nil, err
		}
		switch {
		case goal == nil && x == nil:
			return nil, nil
		case goal == nil:
			if !force {
				return nil, errors.Errorf("cannot clear volume without force=true")
			}
		case x == nil:
		case goal.Equals(*x):
		default:
			hasAncestor, err := gotvc.IsDescendentOf(ctx, src.VCStore, *goal, *x)
			if err != nil {
				return nil, err
			}
			if !force && !hasAncestor {
				return nil, errors.Errorf("cannot CAS, dst ref is not parent of src ref")
			}
		}
		if err := syncStores(ctx, src.StoreTriple(), dst.StoreTriple(), *goal); err != nil {
			return nil, err
		}
		return goal, nil
	})
}

func getSnapshot(ctx context.Context, c cells.Cell) (*Snap, error) {
	const maxSnapshotSize = 4096
	buf := [maxSnapshotSize]byte{}
	n, err := c.Read(ctx, buf[:])
	if err != nil {
		return nil, err
	}
	data := buf[:n]
	if len(data) == 0 {
		return nil, nil
	}
	var x Snap
	if err := json.Unmarshal(data, &x); err != nil {
		return nil, err
	}
	return &x, nil
}

func applySnapshot(ctx context.Context, c cells.Cell, fn func(*Snap) (*Snap, error)) error {
	return cells.Apply(ctx, c, func(xData []byte) ([]byte, error) {
		var xSnap *Snap
		if len(xData) > 0 {
			xSnap = &Snap{}
			if err := json.Unmarshal(xData, &xSnap); err != nil {
				return nil, err
			}
		}
		ySnap, err := fn(xSnap)
		if err != nil {
			return nil, err
		}
		if ySnap == nil {
			return nil, nil
		}
		return json.Marshal(*ySnap)
	})
}

// StoreTriple is an instance of the 3 stores needed to store a Got Snapshot
type StoreTriple struct {
	VC, FS, Raw cadata.Store
}

func syncStores(ctx context.Context, src, dst StoreTriple, snap gotvc.Snapshot) error {
	logrus.Println("begin syncing stores")
	defer logrus.Println("done syncing stores")
	return gotvc.Sync(ctx, src.VC, dst.VC, snap, func(root gotfs.Root) error {
		return gotfs.Sync(ctx, src.FS, src.Raw, dst.FS, dst.Raw, root)
	})
}

func CleanupVolume(ctx context.Context, vol Volume) error {
	start, err := getSnapshot(ctx, vol.Cell)
	if err != nil {
		return err
	}
	ss := [3]cadata.Store{
		vol.VCStore,
		vol.FSStore,
		vol.RawStore,
	}
	keep := [3]stores.MemSet{{}, {}, {}}
	if start != nil {
		if err := gotvc.Populate(ctx, ss[0], *start, keep[0], func(root gotfs.Root) error {
			return gotfs.Populate(ctx, ss[1], root, keep[1], keep[2])
		}); err != nil {
			return err
		}
	}
	for i := range keep {
		logrus.Printf("keeping %d blobs", keep[i].Count())
		if count, err := filterStore(ctx, ss[i], keep[i]); err != nil {
			return err
		} else {
			logrus.Printf("deleted %d blobs", count)
		}
	}
	return nil
}

func filterStore(ctx context.Context, s cadata.Store, set cadata.Set) (int, error) {
	var count int
	err := cadata.ForEach(ctx, s, cadata.Span{}, func(id cadata.ID) error {
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
