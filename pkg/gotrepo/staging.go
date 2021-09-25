package gotrepo

import (
	"context"
	"path"
	"sort"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/gotvc/got/pkg/gotvc"
	"github.com/gotvc/got/pkg/porting"
	"github.com/gotvc/got/pkg/stores"
	"github.com/sirupsen/logrus"
)

func (r *Repo) Commit(ctx context.Context, snapInfo gotvc.SnapInfo) error {
	if yes, err := r.tracker.IsEmpty(ctx); err != nil {
		return err
	} else if yes {
		logrus.Warn("nothing to commit")
		return nil
	}
	_, branch, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	src := r.stagingTriple()
	dst := branch.Volume.StoreTriple()
	// writes go to src, but reads from src should fallback to dst
	src = branches.StoreTriple{
		Raw: stores.AddWriteLayer(dst.Raw, src.Raw),
		FS:  stores.AddWriteLayer(dst.FS, src.FS),
		VC:  stores.AddWriteLayer(dst.VC, src.VC),
	}
	fsop := r.getFSOp(branch)
	vcop := r.getVCOp(branch)
	err = branches.Apply(ctx, *branch, src, func(x *Snap) (*Snap, error) {
		var root *Root
		if x != nil {
			root = &x.Root
		}
		logrus.Println("begin processing tracked paths")
		nextRoot, err := r.applyTrackerChanges(ctx, fsop, src.FS, src.Raw, root)
		if err != nil {
			return nil, err
		}
		logrus.Println("done processing tracked paths")
		if err != nil {
			return nil, err
		}
		return vcop.NewSnapshot(ctx, src.VC, x, *nextRoot, snapInfo)
	})
	if err != nil {
		return err
	}
	return r.tracker.Clear(ctx)
}

func (r *Repo) stagingStore() cadata.Store {
	return r.storeManager.GetStore(0)
}

func (r *Repo) stagingTriple() branches.StoreTriple {
	return branches.StoreTriple{
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
	ads := stores.NewAsyncStore(ds, 32)
	porter := porting.NewPorter(fsop, r.workingDir, nil)
	stage := newStage(fsop, ms, ads)
	if err := r.tracker.ForEach(ctx, func(target string) error {
		return stage.Add(ctx, porter, target)
	}); err != nil {
		return nil, err
	}
	root, err := stage.Apply(ctx, root)
	if err != nil {
		return nil, err
	}
	if err := ads.Close(); err != nil {
		return nil, err
	}
	return root, nil
}

type stage struct {
	gotfs  *gotfs.Operator
	ms, ds Store

	changes map[string]gotfs.Root
}

func newStage(fsop *gotfs.Operator, ms, ds Store) *stage {
	return &stage{
		gotfs:   fsop,
		ms:      ms,
		ds:      ds,
		changes: make(map[string]gotfs.Root),
	}
}

func (s *stage) Add(ctx context.Context, porter porting.Porter, p string) error {
	pathRoot, err := porter.ImportPath(ctx, s.ms, s.ds, p)
	if err != nil {
		return err
	}
	s.changes[p] = *pathRoot
	return nil
}

func (s *stage) Rm(ctx context.Context, p string) error {
	emptyRoot, err := s.gotfs.NewEmpty(ctx, s.ms)
	if err != nil {
		return err
	}
	s.changes[p] = *emptyRoot
	return nil
}

func (s *stage) Apply(ctx context.Context, base *gotfs.Root) (*gotfs.Root, error) {
	if base == nil {
		var err error
		base, err = s.gotfs.NewEmpty(ctx, s.ms)
		if err != nil {
			return nil, err
		}
	}
	var segs []gotfs.Segment
	for _, p := range sortedMapKeys(s.changes) {
		pathRoot := s.changes[p]
		if !gotfs.IsEmpty(pathRoot) {
			var err error
			base, err = s.gotfs.MkdirAll(ctx, s.ms, *base, path.Dir(p))
			if err != nil {
				return nil, err
			}
		}
		segRoot, err := s.gotfs.AddPrefix(ctx, s.ms, p, pathRoot)
		if err != nil {
			return nil, err
		}
		segs = append(segs, gotfs.Segment{
			Root: *segRoot,
			Span: gotfs.SpanForPath(p),
		})
	}
	segs = prepareChanges(*base, segs)
	logrus.Println("splicing...")
	root, err := s.gotfs.Splice(ctx, s.ms, s.ds, segs)
	if err != nil {
		return nil, err
	}
	logrus.Println("done splicing.")
	return root, nil
}

// prepareChanges ensures that the segments represent the whole key space, using base to fill in any gaps.
func prepareChanges(base gotfs.Root, changes []gotfs.Segment) []gotfs.Segment {
	var segs []gotfs.Segment
	for i := range changes {
		// create the span to reference the root, should be inbetween the two entries from segs
		var baseSpan gotkv.Span
		if i > 0 {
			baseSpan.Start = segs[len(segs)-1].Span.End
		}
		baseSpan.End = changes[i].Span.Start
		baseSeg := gotfs.Segment{Root: base, Span: baseSpan}

		segs = append(segs, baseSeg)
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

func sortedMapKeys(x map[string]gotfs.Root) []string {
	var keys []string
	for k := range x {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
