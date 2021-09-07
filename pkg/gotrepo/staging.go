package gotrepo

import (
	"context"
	"log"
	"path"
	"time"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/gotvc/got/pkg/gotvc"
	"github.com/gotvc/got/pkg/porting"
	"github.com/gotvc/got/pkg/stores"
)

// SnapInfo is additional information that can be attached to a snapshot
type SnapInfo struct {
	Message   string
	CreatedAt *time.Time
}

func (r *Repo) Commit(ctx context.Context, snapInfo SnapInfo) error {
	if yes, err := r.tracker.IsEmpty(ctx); err != nil {
		return err
	} else if yes {
		log.Println("WARN: nothing to commit")
		return nil
	}
	_, branch, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	src := r.stagingTriple()
	dst := branch.Volume.StoreTriple()
	// writes go to src, but reads from src should fallback to dst
	src = branches.Triple{
		Raw: stores.AddWriteLayer(dst.Raw, src.Raw),
		FS:  stores.AddWriteLayer(dst.FS, src.FS),
		VC:  stores.AddWriteLayer(dst.VC, src.VC),
	}
	fsop := r.getFSOp(branch)
	err = branches.Apply(ctx, *branch, src, func(x *Snap) (*Snap, error) {
		y, err := gotvc.Change(ctx, src.VC, x, func(root *Root) (*Root, error) {
			log.Println("begin processing tracked paths")
			nextRoot, err := r.applyTrackerChanges(ctx, fsop, src.FS, src.Raw, root)
			if err != nil {
				return nil, err
			}
			log.Println("done processing tracked paths")
			return nextRoot, nil
		})
		if err != nil {
			return nil, err
		}
		y.CreatedAt = snapInfo.CreatedAt
		y.Message = snapInfo.Message
		return y, nil
	})
	if err != nil {
		return err
	}
	return r.tracker.Clear(ctx)
}

func (r *Repo) stagingStore() cadata.Store {
	return r.storeManager.GetStore(0)
}

func (r *Repo) stagingTriple() branches.Triple {
	return branches.Triple{
		VC:  r.stagingStore(),
		FS:  r.stagingStore(),
		Raw: r.stagingStore(),
	}
}

func (r *Repo) StagingStore() cadata.Store {
	return r.stagingStore()
}

// applyTrackerChanges iterates through all the tracked paths and adds or deletes them from root
// the new root, reflecting all of the changes indicated by the tracker, is returned.
func (r *Repo) applyTrackerChanges(ctx context.Context, fsop *gotfs.Operator, ms, ds cadata.Store, root *Root) (*Root, error) {
	if root == nil {
		var err error
		root, err = fsop.NewEmpty(ctx, ms)
		if err != nil {
			return nil, err
		}
	}
	var changes []gotfs.Segment
	if err := r.tracker.ForEach(ctx, func(target string) error {
		pathRoot, err := porting.ImportPath(ctx, fsop, ms, ds, r.workingDir, target)
		if err != nil {
			return err
		}
		if !gotfs.IsEmpty(*pathRoot) {
			root, err = fsop.MkdirAll(ctx, ms, *root, path.Dir(target))
			if err != nil {
				return err
			}
		}
		pathRoot, err = fsop.AddPrefix(ctx, ms, target, *pathRoot)
		if err != nil {
			return err
		}
		changes = append(changes, gotfs.Segment{
			Root: *pathRoot,
			Span: gotfs.SpanForPath(target),
		})
		return nil
	}); err != nil {
		return nil, err
	}
	segs := prepareChanges(*root, changes)
	return fsop.Splice(ctx, ms, ds, segs)
}

// prepareChanges ensures that the segments represent the whole key space, using base to fill in any gaps.
func prepareChanges(base gotfs.Root, changes []gotfs.Segment) []gotfs.Segment {
	var segs []gotfs.Segment
	for i := range changes {
		// create the span to reference the root, should be inbetween the two entries from segs
		var span gotkv.Span
		if i > 0 {
			span.Start = segs[i-1].Span.End
		}
		span.End = changes[i].Span.Start
		segs = append(segs, gotfs.Segment{Root: base, Span: span})
		segs = append(segs, changes[i])
	}
	if len(segs) > 0 {
		segs = append(segs, gotfs.Segment{
			Root: base,
			Span: gotkv.Span{
				Start: segs[len(segs)-1].Span.End,
				End:   nil,
			},
		})
	}
	return segs
}

// func (r *Repo) forEachLeaf(ctx context.Context, fsop *gotfs.Operator, ms Store, root *Root, target string, fn func(p string) error) error {
// 	eg := errgroup.Group{}
// 	inWorking := make(chan string)
// 	inSnapshot := make(chan string)
// 	eg.Go(func() error {
// 		defer close(inWorking)
// 		err := posixfs.WalkLeaves(ctx, r.workingDir, target, func(p string, _ posixfs.DirEnt) error {
// 			inWorking <- p
// 			return nil
// 		})
// 		if posixfs.IsErrNotExist(err) {
// 			err = nil
// 		}
// 		return err
// 	})
// 	eg.Go(func() error {
// 		defer close(inSnapshot)
// 		if root == nil {
// 			return nil
// 		}
// 		return fsop.ForEachFile(ctx, ms, *root, target, func(p string, _ *gotfs.Metadata) error {
// 			inSnapshot <- p
// 			return nil
// 		})
// 	})
// 	eg.Go(func() error {
// 		var p1, p2 *string
// 		// while both are open
// 		for {
// 			if p1 == nil {
// 				if p, open := <-inWorking; open {
// 					p1 = &p
// 				}
// 			}
// 			if p2 == nil {
// 				if p, open := <-inSnapshot; open {
// 					p2 = &p
// 				}
// 			}

// 			var p string
// 			switch {
// 			case p1 != nil && p2 != nil:
// 				if *p1 < *p2 {
// 					p = *p1
// 					p1 = nil
// 				} else {
// 					p = *p2
// 					p2 = nil
// 				}
// 			case p1 != nil:
// 				p = *p1
// 				p1 = nil
// 			case p2 != nil:
// 				p = *p2
// 				p2 = nil
// 			default:
// 				return nil
// 			}
// 			if err := fn(p); err != nil {
// 				return err
// 			}
// 		}
// 	})
// 	return eg.Wait()
// }
