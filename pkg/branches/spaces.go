package branches

import (
	"context"
	"sync"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/cells"
	"github.com/pkg/errors"
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

// A Space holds named branches.
type Space interface {
	Get(ctx context.Context, name string) (*Branch, error)
	Create(ctx context.Context, name string) (*Branch, error)
	Delete(ctx context.Context, name string) error
	ForEach(ctx context.Context, fn func(string) error) error
}

func CreateIfNotExists(ctx context.Context, r Space, k string) (*Branch, error) {
	branch, err := r.Get(ctx, k)
	if err != nil {
		if IsNotExist(err) {
			return r.Create(ctx, k)
		}
		return nil, err
	}
	return branch, nil
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

func (r *MemSpace) Create(ctx context.Context, name string) (*Branch, error) {
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

func (r *MemSpace) ForEach(ctx context.Context, fn func(string) error) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for name := range r.branches {
		if err := fn(name); err != nil {
			return err
		}
	}
	return nil
}

func copyAnotations(x map[string]string) map[string]string {
	y := make(map[string]string, len(x))
	for k, v := range x {
		y[k] = v
	}
	return y
}
