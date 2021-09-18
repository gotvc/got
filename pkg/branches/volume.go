package branches

import (
	"context"
	"encoding/json"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/cells"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotvc"
	"github.com/gotvc/got/pkg/stores"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Snap = gotvc.Snap

// Volume is a Cell and a set of stores
type Volume struct {
	cells.Cell
	VCStore, FSStore, RawStore cadata.Store
}

func (v Volume) StoreTriple() Triple {
	return Triple{
		Raw: v.RawStore,
		FS:  v.FSStore,
		VC:  v.VCStore,
	}
}

func SyncVolumes(ctx context.Context, dst, src Volume, force bool) error {
	return applySnapshot(ctx, dst.Cell, func(x *gotvc.Snapshot) (*gotvc.Snapshot, error) {
		goal, err := getSnapshot(ctx, src.Cell)
		if err != nil {
			return nil, err
		}
		if x == nil {
			return goal, err
		}
		goalRef, err := gotvc.PostSnapshot(ctx, cadata.Void{}, *goal)
		if err != nil {
			return nil, err
		}
		xRef, err := gotvc.PostSnapshot(ctx, cadata.Void{}, *x)
		if err != nil {
			return nil, err
		}
		hasAncestor, err := gotvc.HasAncestor(ctx, src.VCStore, *goalRef, *xRef)
		if err != nil {
			return nil, err
		}
		if !force && !hasAncestor {
			return nil, errors.Errorf("cannot CAS, dst ref is not parent of src ref")
		}
		if err := syncStores(ctx, dst.StoreTriple(), src.StoreTriple(), *goal); err != nil {
			return nil, err
		}
		return goal, nil
	})
}

func getSnapshot(ctx context.Context, c cells.Cell) (*Snap, error) {
	data, err := cells.GetBytes(ctx, c)
	if err != nil {
		return nil, err
	}
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
	return cells.Apply(ctx, c, func(data []byte) ([]byte, error) {
		var x *Snap
		if len(data) > 0 {
			x = &Snap{}
			if err := json.Unmarshal(data, &x); err != nil {
				return nil, err
			}
		}
		y, err := fn(x)
		if err != nil {
			return nil, err
		}
		if y == nil {
			return nil, nil
		}
		return json.Marshal(*y)
	})
}

// Triple is an instance of the 3 stores needed to store a Got Snapshot
type Triple struct {
	VC, FS, Raw cadata.Store
}

func syncStores(ctx context.Context, dst, src Triple, snap gotvc.Snapshot) error {
	logrus.Println("begin syncing stores")
	defer logrus.Println("done syncing stores")
	return gotvc.Sync(ctx, dst.VC, src.VC, snap, func(root gotfs.Root) error {
		return gotfs.Sync(ctx, dst.FS, src.FS, root, func(ref gdat.Ref) error {
			return cadata.Copy(ctx, dst.Raw, src.Raw, ref.CID)
		})
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
		if err := reachableVC(ctx, ss[0], *start, keep[0], func(root gotfs.Root) error {
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
	err := cadata.ForEach(ctx, s, func(id cadata.ID) error {
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

func reachableVC(ctx context.Context, s cadata.Store, start gotvc.Snapshot, set stores.Set, fn func(root gotfs.Root) error) error {
	return gotvc.ForEachAncestor(ctx, s, start, func(ref gdat.Ref, snap gotvc.Snapshot) error {
		if err := fn(snap.Root); err != nil {
			return err
		}
		return set.Add(ctx, ref.CID)
	})
}
