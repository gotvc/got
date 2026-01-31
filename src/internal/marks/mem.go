package marks

import (
	"context"
	"errors"
	"iter"
	"slices"
	"sync"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/tai64"
	"golang.org/x/exp/maps"
)

var _ Space = (*MemSpace)(nil)
var _ SpaceTx = (*memSpaceTx)(nil)

var errReadOnly = errors.New("marks: read-only transaction")

type MemSpace struct {
	mu      sync.RWMutex
	infos   map[string]Info
	targets map[string]gdat.Ref
	store   stores.RWD
}

func NewMem() Space {
	return &MemSpace{
		infos:   map[string]Info{},
		targets: map[string]gdat.Ref{},
		store:   stores.NewMem(),
	}
}

func (r *MemSpace) Do(ctx context.Context, modify bool, fn func(SpaceTx) error) error {
	if modify {
		r.mu.Lock()
		defer r.mu.Unlock()
	} else {
		r.mu.RLock()
		defer r.mu.RUnlock()
	}
	return fn(&memSpaceTx{space: r, modify: modify})
}

func (r *MemSpace) inspectLocked(ctx context.Context, name string) (*Info, error) {
	info, exists := r.infos[name]
	if !exists {
		return nil, ErrNotExist
	}
	info = info.Clone()
	return &info, nil
}

func (r *MemSpace) createLocked(ctx context.Context, name string, cfg Metadata) (*Info, error) {
	if err := CheckName(name); err != nil {
		return nil, err
	}
	if _, exists := r.infos[name]; exists {
		return nil, ErrExists
	}
	info := cfg.AsInfo()
	info.CreatedAt = tai64.Now().TAI64()
	r.infos[name] = info

	info = info.Clone()
	return &info, nil
}

func (r *MemSpace) setLocked(ctx context.Context, name string, cfg Metadata) error {
	if _, exists := r.infos[name]; !exists {
		return ErrNotExist
	}
	info := r.infos[name]
	info.Annotations = slices.Clone(cfg.Annotations)
	r.infos[name] = info
	return nil
}

func (r *MemSpace) deleteLocked(ctx context.Context, name string) error {
	delete(r.infos, name)
	delete(r.targets, name)
	return nil
}

func (r *MemSpace) listLocked() (ret []string) {
	keys := maps.Keys(r.infos)
	slices.Sort(keys)
	for _, name := range keys {
		ret = append(ret, name)
	}
	return ret
}

type memSpaceTx struct {
	space  *MemSpace
	modify bool
}

func (tx *memSpaceTx) Create(ctx context.Context, name string, md Metadata) (*Info, error) {
	tx.space.mu.Lock()
	defer tx.space.mu.Unlock()
	return tx.space.createLocked(ctx, name, md)
}

func (tx *memSpaceTx) All(context.Context) iter.Seq2[string, error] {
	names := tx.space.listLocked()
	return func(yield func(string, error) bool) {
		for _, name := range names {
			if !yield(name, nil) {
				return
			}
		}
	}
}

func (tx *memSpaceTx) Inspect(ctx context.Context, name string) (*Info, error) {
	return tx.space.inspectLocked(ctx, name)
}

func (tx *memSpaceTx) Delete(ctx context.Context, name string) error {
	if !tx.modify {
		return errReadOnly
	}
	return tx.space.deleteLocked(ctx, name)
}

func (tx *memSpaceTx) SetMetadata(ctx context.Context, name string, md Metadata) error {
	if !tx.modify {
		return errReadOnly
	}
	return tx.space.setLocked(ctx, name, md)
}

func (tx *memSpaceTx) Stores() [3]stores.RW {
	return [3]stores.RW{
		tx.space.store,
		tx.space.store,
		tx.space.store,
	}
}

func (tx *memSpaceTx) GetTarget(ctx context.Context, name string, ref *gdat.Ref) (bool, error) {
	var ok bool
	*ref, ok = tx.space.targets[name]
	return ok, nil
}

func (tx *memSpaceTx) SetTarget(ctx context.Context, name string, ref gdat.Ref) error {
	if !tx.modify {
		return errReadOnly
	}
	tx.space.mu.Lock()
	defer tx.space.mu.Unlock()
	_, exists := tx.space.infos[name]
	if !exists {
		return ErrNotExist
	}
	tx.space.targets[name] = ref
	return nil
}
