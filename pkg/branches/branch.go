package branches

import (
	"context"
	"regexp"
	"time"

	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotvc"
	"github.com/pkg/errors"
)

type Annotations = map[string]string

// Branch is a Volume plus additional metadata
type Branch struct {
	Volume      Volume
	Annotations Annotations
	CreatedAt   time.Time
	Salt        []byte
}

var nameRegExp = regexp.MustCompile(`^[\w- =.]+$`)

func IsValidName(name string) bool {
	return nameRegExp.MatchString(name)
}

func CheckName(name string) error {
	if IsValidName(name) {
		return nil
	}
	return errors.Errorf("%q is not a valid branch name", name)
}

// SetHead forcibly sets the head of the branch.
func SetHead(ctx context.Context, b Branch, src StoreTriple, snap Snap) error {
	return applySnapshot(ctx, b.Volume.Cell, func(s *Snap) (*Snap, error) {
		if err := syncStores(ctx, b.Volume.StoreTriple(), src, snap); err != nil {
			return nil, err
		}
		return &snap, nil
	})
}

// GetHead returns the branch head
func GetHead(ctx context.Context, b Branch) (*Snap, error) {
	return getSnapshot(ctx, b.Volume.Cell)
}

// Apply applies fn to branch, any missing data will be pulled from scratch
func Apply(ctx context.Context, b Branch, src StoreTriple, fn func(*Snap) (*Snap, error)) error {
	return applySnapshot(ctx, b.Volume.Cell, func(x *Snap) (*Snap, error) {
		y, err := fn(x)
		if err != nil {
			return nil, err
		}
		if y != nil {
			if err := syncStores(ctx, b.Volume.StoreTriple(), src, *y); err != nil {
				return nil, err
			}
		}
		return y, nil
	})
}

func History(ctx context.Context, b Branch, vcop *gotvc.Operator, fn func(ref gdat.Ref, snap Snap) error) error {
	snap, err := GetHead(ctx, b)
	if err != nil {
		return err
	}
	if snap == nil {
		return nil
	}
	ref := vcop.RefFromSnapshot(*snap)
	if err := fn(ref, *snap); err != nil {
		return err
	}
	return gotvc.ForEach(ctx, b.Volume.VCStore, *snap.Parent, fn)
}
