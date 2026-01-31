package gotrepo

import (
	"context"
	"fmt"
	"strings"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/internal/marks"
)

const (
	nameMaster = "master"
)

type MarkInfo = marks.Info

// FQM represents a fully qualified Mark name.
type FQM struct {
	Space string `json:"space"`
	Name  string `json:"name"`
}

func ParseFQName(s string) FQM {
	parts := strings.SplitN(s, ":", 2)
	switch len(parts) {
	case 1:
		return FQM{Name: parts[0]}
	case 2:
		return FQM{Space: parts[0], Name: parts[1]}
	default:
		panic(s)
	}
}

// CreateBranch creates a new mark in the repo's local space.
func (r *Repo) CreateMark(ctx context.Context, fqname FQM, mcfg marks.DSConfig, anns []marks.Annotation) (*MarkInfo, error) {
	if err := marks.CheckName(fqname.Name); err != nil {
		return nil, err
	}
	space, err := r.GetSpace(ctx, fqname.Space)
	if err != nil {
		return nil, err
	}
	var info *marks.Info
	err = space.Do(ctx, true, func(st marks.SpaceTx) error {
		info, err = st.Create(ctx, fqname.Name, marks.Metadata{Config: mcfg, Annotations: anns})
		return err
	})
	return info, err
}

func (r *Repo) InspectMark(ctx context.Context, fqname FQM) (*marks.Info, error) {
	space, err := r.GetSpace(ctx, fqname.Space)
	if err != nil {
		return nil, err
	}
	var info *marks.Info
	err = space.Do(ctx, false, func(st marks.SpaceTx) error {
		var err error
		info, err = st.Inspect(ctx, fqname.Name)
		return err
	})
	return info, err
}

// DeleteBranch deletes a mark
// The target of the mark may be garbage collected if nothing else
// references it.
func (r *Repo) DeleteMark(ctx context.Context, fqname FQM) error {
	space, err := r.GetSpace(ctx, fqname.Space)
	if err != nil {
		return err
	}
	return space.Do(ctx, true, func(st marks.SpaceTx) error {
		return st.Delete(ctx, fqname.Name)
	})
}

// ConfigureMark adjusts metadata
func (r *Repo) ConfigureMark(ctx context.Context, fqname FQM, md marks.Metadata) error {
	space, err := r.GetSpace(ctx, fqname.Space)
	if err != nil {
		return err
	}
	return space.Do(ctx, true, func(st marks.SpaceTx) error {
		return st.SetMetadata(ctx, fqname.Name, md)
	})
}

// ForEachBranch calls fn once for each branch, or until an error is returned from fn
func (r *Repo) ForEachMark(ctx context.Context, spaceName string, fn func(string) error) error {
	space, err := r.GetSpace(ctx, spaceName)
	if err != nil {
		return err
	}
	return space.Do(ctx, false, func(st marks.SpaceTx) error {
		return marks.ForEach(ctx, st, marks.TotalSpan(), fn)
	})
}

// ViewMark calls fn with a read-only MarkTx
func (r *Repo) ViewMark(ctx context.Context, fqm FQM, fn func(*marks.MarkTx) error) error {
	space, err := r.GetSpace(ctx, fqm.Space)
	if err != nil {
		return err
	}
	return space.Do(ctx, false, func(tx marks.SpaceTx) error {
		mtx, err := marks.NewMarkTx(ctx, tx, fqm.Name)
		if err != nil {
			return err
		}
		return fn(mtx)
	})
}

// MarkLoad loads the Snapshot that the mark points to.
// If the mark is empty then the snapshot will be nil
func (r *Repo) MarkLoad(ctx context.Context, fqm FQM) (*Snap, error) {
	var exists bool
	var snap marks.Snap
	if err := r.ViewMark(ctx, fqm, func(mt *marks.MarkTx) error {
		var err error
		exists, err = mt.Load(ctx, &snap)
		return err
	}); err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	return &snap, nil
}

// CloneMark creates a new branch called next and sets its head to match base's
// TODO: currently marks can only be cloned within the same space.
func (r *Repo) CloneMark(ctx context.Context, base, next FQM) error {
	if base.Space == next.Space {
		space, err := r.GetSpace(ctx, base.Space)
		if err != nil {
			return err
		}
		return space.Do(ctx, true, func(st marks.SpaceTx) error {
			baseInfo, err := st.Inspect(ctx, base.Name)
			if err != nil {
				return err
			}
			if _, err := st.Create(ctx, next.Name, baseInfo.AsMetadata()); err != nil {
				return err
			}
			var ref gdat.Ref
			if ok, err := st.GetTarget(ctx, base.Name, &ref); err != nil {
				return err
			} else if !ok {
				ref = gdat.Ref{}
			}
			return st.SetTarget(ctx, next.Name, ref)
		})
	} else {
		return fmt.Errorf("marks can only be cloned in the same space")
	}
}

// Modify calls fn to modify the target of a Mark.
func (r *Repo) Modify(ctx context.Context, fqm FQM, fn func(mc marks.ModifyCtx) (*Snap, error)) error {
	space, err := r.GetSpace(ctx, fqm.Space)
	if err != nil {
		return err
	}
	return space.Do(ctx, true, func(tx marks.SpaceTx) error {
		mtx, err := marks.NewMarkTx(ctx, tx, fqm.Name)
		if err != nil {
			return err
		}
		return mtx.Modify(ctx, fn)
	})
}
