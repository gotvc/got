package marks

import (
	"context"
	"fmt"
	"iter"
	"regexp"
	"runtime"

	"errors"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotvc"
	"github.com/gotvc/got/src/internal/stores"
	"golang.org/x/sync/errgroup"
)

var (
	ErrNotExist = errors.New("mark does not exist")
	ErrExists   = errors.New("a mark already exists by that name")
)

func IsNotExist(err error) bool {
	return errors.Is(err, ErrNotExist)
}

func IsExists(err error) bool {
	return errors.Is(err, ErrExists)
}

var nameRegExp = regexp.MustCompile(`^[\w-/=_.]+$`)

const MaxNameLen = 1024

type ErrInvalidName struct {
	Name   string
	Reason string
}

func (e ErrInvalidName) Error() string {
	return fmt.Sprintf("invalid mark name: %q reason: %v", e.Name, e.Reason)
}

func CheckName(name string) error {
	if len(name) > MaxNameLen {
		return ErrInvalidName{
			Name:   name,
			Reason: "too long",
		}
	}
	if !nameRegExp.MatchString(name) {
		return ErrInvalidName{
			Name:   name,
			Reason: "contains invalid characters (must match " + nameRegExp.String() + " )",
		}
	}
	return nil
}

type Span struct {
	Begin string
	End   string
}

func TotalSpan() Span {
	return Span{}
}

func (s Span) Contains(x string) bool {
	return s.Begin <= x && (s.End == "" || s.End > x)
}

// A Space holds named marks.
type Space interface {
	// Do calls fn to perform a transaction.
	Do(ctx context.Context, modify bool, fn func(SpaceTx) error) error
}

// SpaceTx is a transaction on a Space
type SpaceTx interface {
	// Create creates a new Mark at name in the Space.
	// The mark will have md for initial metadata.
	// An error is returned if the name already exists.
	Create(ctx context.Context, name string, md Metadata) (*Info, error)
	// Inspect returns all the info for a Mark
	Inspect(ctx context.Context, name string) (*Info, error)
	// SetMetadata sets the metadata for the Mark at name to md
	SetMetadata(ctx context.Context, name string, md Metadata) error
	// Delete deletes a Mark and all of it's metadata, the Snapshot is not removed.
	Delete(ctx context.Context, name string) error
	// All iterates over all the mark names.
	All(context.Context) iter.Seq2[string, error]

	// Store returns the space's underlying stores
	// These can all be the same store, but each will be passed to different systems.
	// 0: GotFS data stream
	// 1: GotFS metadata
	// 2: GotVC
	Stores() [3]stores.RW
	// SetTarget changes the mark so it points to a different snapshot
	SetTarget(ctx context.Context, name string, ref gdat.Ref) error
	// GetTarget retrieves the Snapshot referenced by gdat.Ref
	GetTarget(ctx context.Context, name string, dst *gdat.Ref) (bool, error)
}

// GetSnapshot reads a snapshot from the store.
func GetSnapshot(ctx context.Context, s stores.Reading, ref gdat.Ref) (*Snap, error) {
	vcmach := gotvc.NewMachine(ParsePayload)
	return vcmach.GetSnapshot(ctx, s, ref)
}

func CreateIfNotExists(ctx context.Context, stx SpaceTx, k string, cfg Metadata) (*Info, error) {
	mark, err := stx.Inspect(ctx, k)
	if err != nil {
		if IsNotExist(err) {
			return stx.Create(ctx, k, cfg)
		}
		return nil, err
	}
	return mark, nil
}

// ForEach is a convenience function which uses Space.List to call fn with
// all the mark names contained in span.
func ForEach(ctx context.Context, stx SpaceTx, span Span, fn func(string) error) (retErr error) {
	for name := range stx.All(ctx) {
		if !span.Contains(name) {
			return fmt.Errorf("marks.ForEach: Space implementation is broken got %s when asking for %v", name, span)
		}
		if err := fn(name); err != nil {
			return err
		}
	}
	return nil
}

// SyncSpacesTask contains parameters needed to
// copy marks from one space to another.
type SyncSpacesTask struct {
	// Src is the space to read from.
	Src Space
	// Dst is the name ofthe space to write to.
	Dst Space

	// Filter is applied to src to determine what to copy.
	// If nil, then all marks are copied.
	Filter func(string) bool
	// MapName is applied to go from names in the Src space, to name in the Dst space.
	MapName func(string) string
}

func SyncSpaces(ctx context.Context, task SyncSpacesTask) error {
	return task.Src.Do(ctx, false, func(src SpaceTx) error {
		return task.Dst.Do(ctx, true, func(dst SpaceTx) error {
			nameMap := make(map[string]string)
			for srcName := range src.All(ctx) {
				// filter
				if task.Filter != nil && !task.Filter(srcName) {
					continue
				}
				// map
				dstName := srcName
				if task.MapName != nil {
					dstName = task.MapName(srcName)
				}
				nameMap[srcName] = dstName
			}
			var eg errgroup.Group
			eg.SetLimit(runtime.GOMAXPROCS(0))
			for srcName, dstName := range nameMap {
				srcMark, err := NewMarkTx(ctx, src, srcName)
				if err != nil {
					return err
				}
				md := srcMark.info.AsMetadata()
				if _, err := dst.Create(ctx, dstName, md); err != nil && !IsExists(err) {
					return err
				}
				dstMark, err := NewMarkTx(ctx, dst, dstName)
				if err != nil {
					return err
				}
				eg.Go(func() error {
					return Sync(ctx, srcMark, dstMark, false)
				})
			}
			return eg.Wait()
		})
	})
}
