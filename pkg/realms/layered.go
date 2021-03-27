package realms

import (
	"context"
)

type layered []Realm

// NewLayered creates a layered CellSpace
func NewLayered(rs ...Realm) Realm {
	return layered(rs)
}

func (r layered) Get(ctx context.Context, k string) (*Volume, error) {
	for i := len(r) - 1; i >= 0; i-- {
		env, err := r[i].Get(ctx, k)
		if err == ErrNotExist {
			continue
		} else if err != nil {
			return nil, err
		}
		return env, nil
	}
	return nil, ErrNotExist
}

func (r layered) List(ctx context.Context, prefix string) ([]string, error) {
	var ids []string
	for _, realm := range r {
		ids2, err := realm.List(ctx, prefix)
		if err != nil {
			return nil, err
		}
		ids = append(ids, ids2...)
	}
	return ids, nil
}
