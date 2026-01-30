package gotrepo

import (
	"context"
	"fmt"
	"strings"

	"github.com/gotvc/got/src/internal/gotjob"
	"github.com/gotvc/got/src/internal/marks"
	"github.com/gotvc/got/src/internal/metrics"
)

// SyncUnit syncs 2 marks by name.
func (r *Repo) SyncUnit(ctx context.Context, src, dst FQM, force bool) error {
	srcSpace, err := r.GetSpace(ctx, src.Space)
	if err != nil {
		return err
	}
	dstSpace, err := r.GetSpace(ctx, dst.Space)
	if err != nil {
		return err
	}
	return dstSpace.Do(ctx, true, func(dstTx marks.SpaceTx) error {
		return srcSpace.Do(ctx, false, func(srcTx marks.SpaceTx) error {
			dstBranch, err := dstTx.Open(ctx, dst.Name)
			if err != nil {
				return err
			}
			ctx, cf := metrics.Child(ctx, "syncing volumes")
			defer cf()
			srcBranch, err := srcTx.Open(ctx, src.Name)
			if err != nil {
				return err
			}
			return marks.Sync(ctx, srcBranch, dstBranch, force)
		})
	})
}

// SyncSpacesTask contains parameters needed to
// copy marks from one space to another.
type SyncSpacesTask struct {
	// Src is name of the space to read from.
	Src string
	// Filter is applied to src to determine what to copy.
	// If nil, then all marks are copied.
	Filter func(string) bool
	// MapName is applied to go from names in the Src space, to name in the Dst space.
	MapName func(string) string
	// Dst is the name of the space to write to.
	Dst string
}

// SyncSpaces executes a SyncSpaceTask
func (r *Repo) SyncSpaces(ctx context.Context, task SyncSpacesTask) error {
	srcSpace, err := r.GetSpace(ctx, task.Src)
	if err != nil {
		return err
	}
	dstSpace, err := r.GetSpace(ctx, task.Dst)
	if err != nil {
		return err
	}
	return marks.SyncSpaces(ctx, marks.SyncSpacesTask{
		Src:     srcSpace,
		Dst:     dstSpace,
		Filter:  task.Filter,
		MapName: task.MapName,
	})
}

func (r *Repo) doSyncTasks(jc *gotjob.Ctx, tasks []SyncSpacesTask) error {
	for _, task := range tasks {
		srcSpace, err := r.GetSpace(jc.Context, task.Src)
		if err != nil {
			return err
		}
		dstSpace, err := r.GetSpace(jc.Context, task.Dst)
		if err != nil {
			return err
		}
		jc2 := jc.Child(fmt.Sprintf("sync-space %q -> %q", task.Src, task.Dst))
		if err := marks.SyncSpaces(jc2.Context, marks.SyncSpacesTask{
			Src:     srcSpace,
			Dst:     dstSpace,
			Filter:  task.Filter,
			MapName: task.MapName,
		}); err != nil {
			return err
		}
	}
	return jc.Wait()
}

func (r *Repo) Fetch(ctx context.Context) error {
	var tasks []SyncSpacesTask
	for _, fcfg := range r.config.Fetch {
		tasks = append(tasks, SyncSpacesTask{
			Src: fcfg.From,
			Filter: func(x string) bool {
				return fcfg.Filter.MatchString(x)
			},
			MapName: func(name string) string {
				name = strings.TrimPrefix(name, fcfg.CutPrefix)
				name = fcfg.AddPrefix + name
				return name
			},
			Dst: "",
		})
	}
	jc := gotjob.New(ctx)
	return r.doSyncTasks(&jc, tasks)
}

// Distribute is the opposite of Fetch.
func (r *Repo) Distribute(ctx context.Context) error {
	var tasks []SyncSpacesTask
	for _, fcfg := range r.config.Dist {
		tasks = append(tasks, SyncSpacesTask{
			Src: "", // local space
			Filter: func(x string) bool {
				return fcfg.Filter.MatchString(x)
			},
			MapName: func(name string) string {
				// this is the reverse of what we do in Fetch
				name = strings.TrimPrefix(name, fcfg.AddPrefix)
				name = fcfg.CutPrefix + name
				return name
			},
			Dst: fcfg.To,
		})
	}
	jc := gotjob.New(ctx)
	return r.doSyncTasks(&jc, tasks)
}
