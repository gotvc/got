package gotcore

import (
	"context"
	"fmt"
	"iter"
	"regexp"
	"runtime"
	"sync"

	"errors"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotfs"
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

type ErrRefIntegrity struct {
	Ref   gdat.Ref
	Store string
}

func (e ErrRefIntegrity) Error() string {
	return fmt.Sprintf("this operation would create a dangling reference ref=%v in the store=%s", e.Ref, e.Store)
}

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

// A Space holds named gotcore.
type Space interface {
	// Do calls fn to perform a transaction.
	Do(ctx context.Context, modify bool, fn func(SpaceTx) error) error
}

// RW are read-write stores
type RW struct {
	FS gotfs.RW
	VC stores.RW
}

func (rw RW) RO() RO {
	return RO{FS: rw.FS.RO(), VC: rw.VC}
}

func (rw RW) WO() WO {
	return WO{FS: rw.FS.WO(), VC: rw.VC}
}

type RO struct {
	FS gotfs.RO
	VC stores.RO
}

type WO struct {
	FS gotfs.WO
	VC stores.WO
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
	// Delete deletes a Mark and all of it's metadata, the Commit is not removed.
	Delete(ctx context.Context, name string) error
	// All iterates over all the mark names.
	All(context.Context) iter.Seq2[string, error]

	// Store returns the space's underlying stores
	// These can all be the same store, but each will be passed to different systems.
	// 0: GotFS data stream
	// 1: GotFS metadata
	// 2: GotVC
	Stores() RW
	// SetTarget changes the mark so it points to a different commit
	SetTarget(ctx context.Context, name string, ref gdat.Ref) error
	// GetTarget retrieves the Commit referenced by gdat.Ref
	// If the name has no ref, then the zero value, and nil should be returned.
	GetTarget(ctx context.Context, name string) (gdat.Ref, error)
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
	for name, err := range stx.All(ctx) {
		if err != nil {
			return err
		}
		if !span.Contains(name) {
			return fmt.Errorf("gotcore.ForEach: Space implementation is broken got %s when asking for %v", name, span)
		}
		if err := fn(name); err != nil {
			return err
		}
	}
	return nil
}

func CloneMark(ctx context.Context, st SpaceTx, from, to string) error {
	baseInfo, err := st.Inspect(ctx, from)
	if err != nil {
		return err
	}
	if _, err := st.Create(ctx, to, baseInfo.AsMetadata()); err != nil {
		return err
	}
	ref, err := st.GetTarget(ctx, from)
	if err != nil {
		return err
	}
	return st.SetTarget(ctx, to, ref)
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
	// If nil, then the mark names in the src are taken as is.
	MapName func(string) string
	// AllowPartial allows some marks to be synced and some to error.
	// A false value (the default) will abort the whole transaction, when 1 sync fail.
	AllowPartial bool
}

type SyncResult struct {
	// Dst is the name of the Mark in the destination space
	Dst string
	// Src is the name of the Mark in the source space
	// If empty, then this was deleted in the dest.
	Src string

	// Err is nil if the sync was successful, non-nil if there was a problem
	Err error
	// Created is set if the mark was created
	Created    bool
	Prev, Next gdat.Ref
}

func (sr SyncResult) IsOK() bool {
	return sr.Err == nil
}

func (sr SyncResult) WasDeleted() bool {
	return sr.IsOK() && sr.Src == ""
}

func (sr SyncResult) WasCreated() bool {
	return sr.IsOK() && sr.Created
}

func (sr SyncResult) WasUpdated() bool {
	return sr.IsOK() && !sr.Prev.Equals(&sr.Next)
}

type SyncErr struct {
	Src, Dst string
	Err      error
}

func (e *SyncErr) Error() string {
	return fmt.Sprintf("while syncing %s -> %s: %v", e.Src, e.Dst, e.Err)
}

func SyncSpaces(ctx context.Context, task SyncSpacesTask) ([]SyncResult, error) {
	var mu sync.Mutex
	var ret []SyncResult
	var hadSuccess bool
	appendResult := func(x SyncResult) {
		mu.Lock()
		defer mu.Unlock()
		ret = append(ret, x)
		if x.Err == nil {
			hadSuccess = true
		}
	}
	err := task.Src.Do(ctx, false, func(src SpaceTx) error {
		return task.Dst.Do(ctx, true, func(dst SpaceTx) error {
			nameMap := make(map[string]string)
			for srcName, err := range src.All(ctx) {
				if err != nil {
					return err
				}
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
				res := SyncResult{Dst: dstName, Src: srcName}
				if _, err := dst.Create(ctx, dstName, md); err != nil && !IsExists(err) {
					return err
				} else if err == nil {
					res.Created = true
				}
				dstMark, err := NewMarkTx(ctx, dst, dstName)
				if err != nil {
					return err
				}
				eg.Go(func() error {
					if err := func() error {
						change, err := Sync(ctx, srcMark, dstMark, false)
						if err != nil {
							return err
						}
						res.Prev = change.Prev
						res.Next = change.Next
						return nil
					}(); err != nil {
						if !task.AllowPartial {
							return &SyncErr{Src: srcName, Dst: dstName, Err: err}
						} else {
							res.Err = err
						}
					}
					appendResult(res)
					return nil
				})
			}
			return eg.Wait()
		})
	})
	if err != nil {
		return nil, err
	}
	if len(ret) > 0 && !hadSuccess {
		return ret, fmt.Errorf("all syncs had errors")
	}
	return ret, nil
}
