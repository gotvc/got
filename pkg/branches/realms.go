package branches

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/cells"
	"github.com/pkg/errors"
)

var (
	ErrNotExist = errors.New("volume does not exist")
	ErrExists   = errors.New("a volume already exists by that name")
)

func IsNotExist(err error) bool {
	return err == ErrNotExist
}

func IsExists(err error) bool {
	return err == ErrExists
}

// Volume is a Cell and a set of stores pair
type Volume struct {
	cells.Cell
	VCStore, FSStore, RawStore cadata.Store
}

type Branch struct {
	Volume      *Volume
	Annotations map[string]string
}

// A Realm is a set of named volumes.
type Realm interface {
	Get(ctx context.Context, name string) (*Branch, error)
	Create(ctx context.Context, name string) error
	Delete(ctx context.Context, name string) error
	ForEach(ctx context.Context, fn func(string) error) error
}

func CreateIfNotExists(ctx context.Context, r Realm, k string) error {
	if _, err := r.Get(ctx, k); err != nil {
		if IsNotExist(err) {
			return r.Create(ctx, k)
		}
		return err
	}
	return nil
}
