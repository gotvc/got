package got

import (
	"context"
	"log"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/gdat"
	"github.com/brendoncarroll/got/pkg/gotfs"
	"github.com/brendoncarroll/got/pkg/gotvc"
	"github.com/brendoncarroll/got/pkg/volumes"
)

func (r *Repo) Cleanup(ctx context.Context, volNames []string) error {
	if len(volNames) == 0 {
		name, _, err := r.GetActiveVolume(ctx)
		if err != nil {
			return err
		}
		volNames = []string{name}
	}
	for _, name := range volNames {
		vol, err := r.GetRealm().Get(ctx, name)
		if err != nil {
			return err
		}
		log.Println("begin cleanup on", name)
		if err := r.cleanupVolume(ctx, vol); err != nil {
			return err
		}
		log.Println("done cleanup on", name)
	}
	return nil
}

func (r *Repo) cleanupVolume(ctx context.Context, vol *volumes.Volume) error {
	start, err := getSnapshot(ctx, vol.Cell)
	if err != nil {
		return err
	}
	stores := [3]cadata.Store{
		vol.VCStore,
		vol.FSStore,
		vol.RawStore,
	}
	keep := [3]cadata.MemSet{{}, {}, {}}
	if start != nil {
		if err := r.reachableVC(ctx, stores[0], *start, keep[0], func(root Root) error {
			return gotfs.Populate(ctx, stores[1], root, keep[1], keep[2])
		}); err != nil {
			return err
		}
	}
	for i := range keep {
		log.Printf("keeping %d blobs", keep[i].Count())
		if count, err := filterStore(ctx, stores[i], keep[i]); err != nil {
			return err
		} else {
			log.Printf("deleted %d blobs", count)
		}
	}
	return nil
}

func filterStore(ctx context.Context, s Store, set cadata.Set) (int, error) {
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

func (r *Repo) reachableVC(ctx context.Context, s Store, start gotvc.Snapshot, set cadata.Set, fn func(root Root) error) error {
	return gotvc.ForEachAncestor(ctx, s, start, func(ref gdat.Ref, snap gotvc.Snapshot) error {
		if err := fn(snap.Root); err != nil {
			return err
		}
		return set.Add(ctx, ref.CID)
	})
}
