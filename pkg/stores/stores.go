package stores

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
)

type (
	Store = cadata.Store
	ID    = cadata.ID
)

type Set interface {
	Exists(ctx context.Context, id ID) (bool, error)
	Add(ctx context.Context, id ID) error
	Delete(ctx context.Context, id ID) error
}

type MemSet map[ID]struct{}

func (ms MemSet) Exists(ctx context.Context, id ID) (bool, error) {
	_, exists := ms[id]
	return exists, nil
}

func (ms MemSet) Add(ctx context.Context, id ID) error {
	ms[id] = struct{}{}
	return nil
}

func (ms MemSet) Delete(ctx context.Context, id ID) error {
	delete(ms, id)
	return nil
}

func (ms MemSet) Count() int {
	return len(ms)
}
