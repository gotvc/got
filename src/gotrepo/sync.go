package gotrepo

import (
	"context"
	"fmt"
	"strings"

	"github.com/gotvc/got/src/internal/gotcore"
	"github.com/gotvc/got/src/internal/gotjob"
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
	// Even if these are the same space, 1 read-only and 1 modify should work.
	return dstSpace.Do(ctx, true, func(dstTx gotcore.SpaceTx) error {
		return srcSpace.Do(ctx, false, func(srcTx gotcore.SpaceTx) error {
			dstMTx, err := gotcore.NewMarkTx(ctx, dstTx, dst.Name)
			if err != nil {
				return err
			}
			ctx, cf := metrics.Child(ctx, "syncing volumes")
			defer cf()
			srcMTx, err := gotcore.NewMarkTx(ctx, srcTx, src.Name)
			if err != nil {
				return err
			}
			return gotcore.Sync(ctx, srcMTx, dstMTx, force)
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
func (r *Repo) SyncSpaces(ctx context.Context, task SyncSpacesTask) ([]gotcore.SyncResult, error) {
	srcSpace, err := r.GetSpace(ctx, task.Src)
	if err != nil {
		return nil, err
	}
	dstSpace, err := r.GetSpace(ctx, task.Dst)
	if err != nil {
		return nil, err
	}
	return gotcore.SyncSpaces(ctx, gotcore.SyncSpacesTask{
		Src:     srcSpace,
		Dst:     dstSpace,
		Filter:  task.Filter,
		MapName: task.MapName,
	})
}

type SyncResult struct {
	// Src is the source space
	// Dst is the destination space
	Src, Dst string
	// Items is each overwritten mark
	Items []gotcore.SyncResult
}

func (r *Repo) doSyncTasks(jc *gotjob.Ctx, tasks []SyncSpacesTask, onDone func(SyncResult)) error {
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
		res, err := gotcore.SyncSpaces(jc2.Context, gotcore.SyncSpacesTask{
			Src:     srcSpace,
			Dst:     dstSpace,
			Filter:  task.Filter,
			MapName: task.MapName,
		})
		if err != nil {
			return err
		}
		if onDone != nil {
			onDone(SyncResult{
				Src:   task.Src,
				Dst:   task.Dst,
				Items: res,
			})
		}
	}
	return jc.Wait()
}

// Pull executes all pull tasks as defined in the configuration
// Pull tasks write to the repo's local namespace.
func (r *Repo) Pull(ctx context.Context, onDone func(SyncResult)) error {
	var tasks []SyncSpacesTask
	for _, fcfg := range r.config.Pull {
		var filter func(string) bool
		if fcfg.Filter != nil {
			filter = fcfg.Filter.MatchString
		}
		excludePrefixes := pullExcludedPrefixes(r.config.Push, fcfg.From)
		if len(excludePrefixes) > 0 || filter != nil {
			filter = chainFilters(filter, func(name string) bool {
				return !hasPrefixIn(excludePrefixes, name)
			})
		}
		tasks = append(tasks, SyncSpacesTask{
			Src:    fcfg.From,
			Filter: filter,
			MapName: func(name string) string {
				name = strings.TrimPrefix(name, fcfg.CutPrefix)
				name = fcfg.AddPrefix + name
				return name
			},
			Dst: "",
		})
	}
	jc := gotjob.New(ctx)
	return r.doSyncTasks(&jc, tasks, onDone)
}

// Push is the opposite of Pull.
func (r *Repo) Push(ctx context.Context, onDone func(SyncResult)) error {
	var tasks []SyncSpacesTask
	for _, fcfg := range r.config.Push {
		var filter func(string) bool
		if fcfg.Filter != nil {
			filter = fcfg.Filter.MatchString
		}
		excludePrefixes := pushExcludedPrefixes(r.config.Pull, fcfg.To)
		if len(excludePrefixes) > 0 || filter != nil {
			filter = chainFilters(filter, func(name string) bool {
				return !hasPrefixIn(excludePrefixes, name)
			})
		}
		tasks = append(tasks, SyncSpacesTask{
			Src:    "", // local space
			Filter: filter,
			MapName: func(name string) string {
				name = strings.TrimPrefix(name, fcfg.CutPrefix)
				name = fcfg.AddPrefix + name
				return name
			},
			Dst: fcfg.To,
		})
	}
	jc := gotjob.New(ctx)
	return r.doSyncTasks(&jc, tasks, onDone)
}

func hasPrefixIn(prefixes []string, x string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(x, p) {
			return true
		}
	}
	return false
}

// pullExcludes returns the set of excluded prefixes for pull
func pullExcludedPrefixes(pcs []PushConfig, from string) (ret []string) {
	for _, pc := range pcs {
		if pc.To != from {
			continue
		}
		if pc.AddPrefix == "" {
			continue
		}
		// AddPrefix will be the prefix in the remote store, since it was
		// added on Pull
		ret = append(ret, pc.AddPrefix)
	}
	return ret
}

// pushExcludes returns the set of excluded prefixes for pull
func pushExcludedPrefixes(pcs []PullConfig, to string) (ret []string) {
	for _, pc := range pcs {
		if pc.From != to {
			continue
		}
		if pc.AddPrefix == "" {
			continue
		}
		// AddPrefix will be the prefix in the local store, since it was
		// added on Pull
		ret = append(ret, pc.AddPrefix)
	}
	return ret
}

func chainFilters(a, b func(string) bool) func(string) bool {
	return func(name string) bool {
		if a != nil && !a(name) {
			return false
		}
		if b != nil && !b(name) {
			return false
		}
		return true
	}
}
