package branches

import (
	"context"
	"sync"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/cells"
	"github.com/brendoncarroll/go-tai64"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

type MemSpace struct {
	newStore func() cadata.Store
	newCell  func() cells.BytesCell

	mu      sync.RWMutex
	infos   map[string]Info
	volumes map[string]Volume
}

func NewMem(newStore func() cadata.Store, newCell func() cells.BytesCell) Space {
	return &MemSpace{
		newStore: newStore,
		newCell:  newCell,
		infos:    map[string]Info{},
		volumes:  make(map[string]Volume),
	}
}

func (r *MemSpace) Get(ctx context.Context, name string) (*Info, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	info, exists := r.infos[name]
	if !exists {
		return nil, ErrNotExist
	}
	info = info.Clone()
	return &info, nil
}

func (r *MemSpace) Create(ctx context.Context, name string, cfg Config) (*Info, error) {
	if err := CheckName(name); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.infos[name]; exists {
		return nil, ErrExists
	}
	r.volumes[name] = Volume{
		Cell:     r.newCell(),
		VCStore:  r.newStore(),
		FSStore:  r.newStore(),
		RawStore: r.newStore(),
	}
	info := cfg.AsInfo()
	info.CreatedAt = tai64.Now().TAI64()
	r.infos[name] = info

	info = info.Clone()
	return &info, nil
}

func (r *MemSpace) Set(ctx context.Context, name string, cfg Config) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.infos[name]; !exists {
		return ErrNotExist
	}
	info := r.infos[name]
	info.Mode = cfg.Mode
	info.Salt = slices.Clone(cfg.Salt)
	info.Annotations = slices.Clone(cfg.Annotations)
	r.infos[name] = info
	return nil
}

func (r *MemSpace) Delete(ctx context.Context, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.infos, name)
	delete(r.volumes, name)
	return nil
}

func (r *MemSpace) List(ctx context.Context, span Span, limit int) (ret []string, _ error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
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

func (r *MemSpace) Open(ctx context.Context, name string) (*Volume, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.volumes[name]; !exists {
		return nil, ErrNotExist
	}
	v := r.volumes[name]
	return &v, nil
}
