package realms

import (
	"context"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/cells"
	"github.com/pkg/errors"
)

var ErrNotExist = errors.New("volume does not exist")
var ErrExists = errors.New("a volume already exists by that name")
var ErrTooMany = errors.Errorf("too many too list")

// Volume is a (Cell, Store) pair
type Volume struct {
	cells.Cell
	cadata.Store
}

// A Realm is a set of named keys, each of which points to a (Cell, Store) pair.
type Realm interface {
	Get(ctx context.Context, name string) (*Volume, error)
	//Create(ctx context.Context, name string) error
	//Delete(ctx context.Context, name string) error
	List(ctx context.Context, prefix string) ([]string, error)
}

func ForEach(ctx context.Context, r Realm, fn func(string) error) error {
	keys, err := r.List(ctx, "")
	if err != nil && err != ErrTooMany {
		return err
	}
	for _, key := range keys {
		if err := fn(key); err != nil {
			return err
		}
	}
	return nil
}
