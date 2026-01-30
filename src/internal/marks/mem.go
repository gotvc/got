package marks

import (
	"context"
	"errors"
	"sync"

	"github.com/gotvc/got/src/internal/volumes"
	"go.brendoncarroll.net/tai64"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

var _ Space = (*MemSpace)(nil)
var _ SpaceTx = (*memSpaceTx)(nil)

var errReadOnly = errors.New("marks: read-only transaction")

type memSpaceTx struct {
	space  *MemSpace
	modify bool
}

type MemSpace struct {
	newVolume func() volumes.Volume

	mu      sync.RWMutex
	infos   map[string]Info
	volumes map[string]Volume
}

func NewMem(newVolume func() volumes.Volume) Space {
	return &MemSpace{
		newVolume: newVolume,
		infos:     map[string]Info{},
		volumes:   make(map[string]Volume),
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
	r.volumes[name] = r.newVolume()
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
	delete(r.volumes, name)
	return nil
}

func (r *MemSpace) listLocked(ctx context.Context, span Span, limit int) (ret []string, _ error) {
	keys := maps.Keys(r.infos)
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

func (r *MemSpace) openLocked(ctx context.Context, name string) (*Mark, error) {
	if _, exists := r.volumes[name]; !exists {
		return nil, ErrNotExist
	}
	info, exists := r.infos[name]
	if !exists {
		return nil, ErrNotExist
	}
	return &Mark{
		Volume: r.volumes[name],
		Info:   info,
	}, nil
}

func (tx *memSpaceTx) Create(ctx context.Context, name string, md Metadata) (*Info, error) {
	return tx.space.createLocked(ctx, name, md)
}

func (tx *memSpaceTx) List(ctx context.Context, span Span, limit int) ([]string, error) {
	return tx.space.listLocked(ctx, span, limit)
}

func (tx *memSpaceTx) Open(ctx context.Context, name string) (*Mark, error) {
	return tx.space.openLocked(ctx, name)
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

func (tx *memSpaceTx) Set(ctx context.Context, name string, cfg Metadata) error {
	if !tx.modify {
		return errReadOnly
	}
	return tx.space.setLocked(ctx, name, cfg)
}
