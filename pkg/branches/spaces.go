package branches

import (
	"context"
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
	return err == ErrNotExist
}

func IsExists(err error) bool {
	return err == ErrExists
}

type Params struct {
	Salt []byte
}

func NewParams(public bool) Params {
	var salt []byte
	if !public {
		salt = make([]byte, 32)
	}
	readRandom(salt)
	return Params{
		Salt: salt,
	}
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
	Get(ctx context.Context, name string) (*Branch, error)
	Create(ctx context.Context, name string, params Params) (*Branch, error)
	Delete(ctx context.Context, name string) error
	List(ctx context.Context, span Span, limit int) ([]string, error)
}

func CreateIfNotExists(ctx context.Context, r Space, k string, params Params) (*Branch, error) {
	branch, err := r.Get(ctx, k)
	if err != nil {
		if IsNotExist(err) {
			return r.Create(ctx, k, params)
		}
		return nil, err
	}
	return branch, nil
}

// ForEach is a convenience function which uses Space.List to call fn with
// all the branch names contained in span.
func ForEach(ctx context.Context, s Space, span Span, fn func(string) error) error {
	for {
		names, err := s.List(ctx, span, 0)
		if err != nil {
			return err
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
	return nil
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
	branch.Annotations = copyAnotations(branch.Annotations)
	if !exists {
		return nil, ErrNotExist
	}
	return &branch, nil
}

func (r *MemSpace) Create(ctx context.Context, name string, params Params) (*Branch, error) {
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
		Salt:      params.Salt,
		CreatedAt: tai64.Now().TAI64(),
	}
	branch := r.branches[name]
	return &branch, nil
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

func copyAnotations(x map[string]string) map[string]string {
	y := make(map[string]string, len(x))
	for k, v := range x {
		y[k] = v
	}
	return y
}
