package gotrepo

import (
	"context"
	"fmt"
	"strings"

	"github.com/gotvc/got/src/internal/gotcore"
)

const (
	nameMaster = "master"
)

type MarkInfo = gotcore.Info

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
func (r *Repo) CreateMark(ctx context.Context, fqname FQM, mcfg gotcore.DSConfig, anns []gotcore.Annotation) (*MarkInfo, error) {
	if err := gotcore.CheckName(fqname.Name); err != nil {
		return nil, err
	}
	space, err := r.GetSpace(ctx, fqname.Space)
	if err != nil {
		return nil, err
	}
	var info *gotcore.Info
	err = space.Do(ctx, true, func(st gotcore.SpaceTx) error {
		info, err = st.Create(ctx, fqname.Name, gotcore.Metadata{Config: mcfg, Annotations: anns})
		return err
	})
	return info, err
}

func (r *Repo) InspectMark(ctx context.Context, fqname FQM) (*gotcore.Info, error) {
	space, err := r.GetSpace(ctx, fqname.Space)
	if err != nil {
		return nil, err
	}
	var info *gotcore.Info
	err = space.Do(ctx, false, func(st gotcore.SpaceTx) error {
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
	return space.Do(ctx, true, func(st gotcore.SpaceTx) error {
		return st.Delete(ctx, fqname.Name)
	})
}

// ConfigureMark adjusts metadata
func (r *Repo) ConfigureMark(ctx context.Context, fqname FQM, md gotcore.Metadata) error {
	space, err := r.GetSpace(ctx, fqname.Space)
	if err != nil {
		return err
	}
	return space.Do(ctx, true, func(st gotcore.SpaceTx) error {
		return st.SetMetadata(ctx, fqname.Name, md)
	})
}

// ForEachBranch calls fn once for each branch, or until an error is returned from fn
func (r *Repo) ForEachMark(ctx context.Context, spaceName string, fn func(name string) error) error {
	space, err := r.GetSpace(ctx, spaceName)
	if err != nil {
		return err
	}
	return space.Do(ctx, false, func(st gotcore.SpaceTx) error {
		return gotcore.ForEach(ctx, st, gotcore.TotalSpan(), fn)
	})
}

// ViewMark calls fn with a read-only MarkTx
func (r *Repo) ViewMark(ctx context.Context, fqm FQM, fn func(*gotcore.MarkTx) error) error {
	space, err := r.GetSpace(ctx, fqm.Space)
	if err != nil {
		return err
	}
	return space.Do(ctx, false, func(tx gotcore.SpaceTx) error {
		mtx, err := gotcore.NewMarkTx(ctx, tx, fqm.Name)
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
	var snap gotcore.Snap
	if err := r.ViewMark(ctx, fqm, func(mt *gotcore.MarkTx) error {
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

func (r *Repo) MoveMark(ctx context.Context, spaceName, from, to string) error {
	space, err := r.GetSpace(ctx, spaceName)
	if err != nil {
		return err
	}
	return space.Do(ctx, true, func(st gotcore.SpaceTx) error {
		if err := gotcore.CloneMark(ctx, st, from, to); err != nil {
			return err
		}
		return st.Delete(ctx, from)
	})
}

// CloneMark creates a new branch called next and sets its head to match base's
// TODO: currently marks can only be cloned within the same space.
func (r *Repo) CloneMark(ctx context.Context, base, next FQM) error {
	if base.Space == next.Space {
		space, err := r.GetSpace(ctx, base.Space)
		if err != nil {
			return err
		}
		return space.Do(ctx, true, func(st gotcore.SpaceTx) error {
			return gotcore.CloneMark(ctx, st, base.Name, next.Name)
		})
	} else {
		return fmt.Errorf("marks can only be cloned in the same space")
	}
}

// Modify calls fn to modify the target of a Mark.
func (r *Repo) Modify(ctx context.Context, fqm FQM, fn func(mc gotcore.ModifyCtx) (*Snap, error)) error {
	space, err := r.GetSpace(ctx, fqm.Space)
	if err != nil {
		return err
	}
	return space.Do(ctx, true, func(tx gotcore.SpaceTx) error {
		mtx, err := gotcore.NewMarkTx(ctx, tx, fqm.Name)
		if err != nil {
			return err
		}
		return mtx.Modify(ctx, fn)
	})
}
