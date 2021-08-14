package gotrepo

import (
	"context"
	"log"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotvc"
	"github.com/gotvc/got/pkg/stores"
)

func (r *Repo) Cleanup(ctx context.Context) error {
	return r.porter.Cleanup(ctx)
}

func (r *Repo) CleanupBranches(ctx context.Context, branchNames []string) error {
	if len(branchNames) == 0 {
		name, _, err := r.GetActiveBranch(ctx)
		if err != nil {
			return err
		}
		branchNames = []string{name}
	}
	for _, name := range branchNames {
		branch, err := r.GetSpace().Get(ctx, name)
		if err != nil {
			return err
		}
		log.Println("begin cleanup on", name)
		if err := r.cleanupVolume(ctx, branch.Volume); err != nil {
			return err
		}
		log.Println("done cleanup on", name)
	}
	return nil
}

func (r *Repo) cleanupVolume(ctx context.Context, vol branches.Volume) error {
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
		if err := r.reachableVC(ctx, ss[0], *start, keep[0], func(root Root) error {
			return gotfs.Populate(ctx, ss[1], root, keep[1], keep[2])
		}); err != nil {
			return err
		}
	}
	for i := range keep {
		log.Printf("keeping %d blobs", keep[i].Count())
		if count, err := filterStore(ctx, ss[i], keep[i]); err != nil {
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

func (r *Repo) reachableVC(ctx context.Context, s Store, start gotvc.Snapshot, set stores.Set, fn func(root Root) error) error {
	return gotvc.ForEachAncestor(ctx, s, start, func(ref gdat.Ref, snap gotvc.Snapshot) error {
		if err := fn(snap.Root); err != nil {
			return err
		}
		return set.Add(ctx, ref.CID)
	})
}
