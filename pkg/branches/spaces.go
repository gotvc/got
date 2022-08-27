package branches

import (
	"context"
	"fmt"
	"regexp"
	"sync"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/cells"
	"github.com/brendoncarroll/go-tai64"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

var (
	ErrNotExist = errors.New("branch does not exist")
	ErrExists   = errors.New("a branch already exists by that name")
)

func IsNotExist(err error) bool {
	return errors.Is(err, ErrNotExist)
}

func IsExists(err error) bool {
	return errors.Is(err, ErrNotExist)
}

var nameRegExp = regexp.MustCompile(`^[\w-/=_.]+$`)

const MaxNameLen = 1024

type ErrInvalidName struct {
	Name   string
	Reason string
}

func (e ErrInvalidName) Error() string {
	return fmt.Sprintf("invalid branch name: %q reason: %v", e.Name, e.Reason)
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

// A Space holds named branches.
type Space interface {
	Create(ctx context.Context, name string, md Metadata) (*Branch, error)
	Get(ctx context.Context, name string) (*Branch, error)
	Set(ctx context.Context, name string, md Metadata) error
	Delete(ctx context.Context, name string) error
	List(ctx context.Context, span Span, limit int) ([]string, error)
}

func CreateIfNotExists(ctx context.Context, r Space, k string, md Metadata) (*Branch, error) {
	branch, err := r.Get(ctx, k)
	if err != nil {
		if IsNotExist(err) {
			return r.Create(ctx, k, md)
		}
		return nil, err
	}
	return branch, nil
}

// ForEach is a convenience function which uses Space.List to call fn with
// all the branch names contained in span.
func ForEach(ctx context.Context, s Space, span Span, fn func(string) error) (retErr error) {
	for {
		names, err := s.List(ctx, span, 0)
		if err != nil {
			retErr = err
		}
		if len(names) == 0 {
			break
		}
		for _, name := range names {
			if err := fn(name); err != nil {
				return err
			}
		}
		span.Begin = names[len(names)-1] + "\x00"
	}
	return retErr
}

type MemSpace struct {
	newStore func() cadata.Store
	newCell  func() cells.Cell

	mu       sync.RWMutex
	branches map[string]Branch
}

func NewMem(newStore func() cadata.Store, newCell func() cells.Cell) Space {
	return &MemSpace{
		newStore: newStore,
		newCell:  newCell,
		branches: map[string]Branch{},
	}
}

func (r *MemSpace) Get(ctx context.Context, name string) (*Branch, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	branch, exists := r.branches[name]
	branch.Metadata = branch.Metadata.Clone()
	if !exists {
		return nil, ErrNotExist
	}
	return &branch, nil
}

func (r *MemSpace) Create(ctx context.Context, name string, md Metadata) (*Branch, error) {
	if err := CheckName(name); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.branches[name]; exists {
		return nil, ErrExists
	}
	r.branches[name] = Branch{
		Volume: Volume{
			Cell:     r.newCell(),
			VCStore:  r.newStore(),
			FSStore:  r.newStore(),
			RawStore: r.newStore(),
		},
		Metadata:  md.Clone(),
		CreatedAt: tai64.Now().TAI64(),
	}
	branch := r.branches[name]
	return &branch, nil
}

func (r *MemSpace) Set(ctx context.Context, name string, md Metadata) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.branches[name]; !exists {
		return ErrNotExist
	}
	b := r.branches[name]
	b.Metadata = md.Clone()
	r.branches[name] = b
	return nil
}

func (r *MemSpace) Delete(ctx context.Context, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.branches, name)
	return nil
}

func (r *MemSpace) List(ctx context.Context, span Span, limit int) (ret []string, _ error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	keys := maps.Keys(r.branches)
	slices.Sort(keys)
	for _, name := range keys {
		if limit > 0 && len(ret) >= limit {
			break
		}
		if span.Contains(name) {
			ret = append(ret, name)
		}
	}
	return ret, nil
}
