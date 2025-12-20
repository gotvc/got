package gotrepo

import (
	"context"

	"github.com/gotvc/got/src/internal/metrics"
	"github.com/gotvc/got/src/marks"
)

// SyncMarks syncs 2 marks by name.
func (r *Repo) SyncMarks(ctx context.Context, src, dst FQM, force bool) error {
	srcSpace, err := r.GetSpace(ctx, src.Space)
	if err != nil {
		return err
	}
	dstSpace, err := r.GetSpace(ctx, dst.Space)
	if err != nil {
		return err
	}
	srcBranch, err := srcSpace.Open(ctx, src.Name)
	if err != nil {
		return err
	}
	dstBranch, err := dstSpace.Open(ctx, dst.Name)
	if err != nil {
		return err
	}
	ctx, cf := metrics.Child(ctx, "syncing volumes")
	defer cf()
	return marks.Sync(ctx, srcBranch, dstBranch, force)
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

// Fetch executes a fetch task.
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
