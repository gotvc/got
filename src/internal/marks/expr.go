package marks

import (
	"context"
	"fmt"
	"strings"

	"github.com/gotvc/got/src/gdat"
)

// SnapExpr is a sum type representing the different ways
// to refer to a Snapshot in Got
// So far, there are 2 primitive ways
// - Exactly by Ref
// - By Mark
// And 1 higher-order way
// - By an offset from the result of a previous expression
type SnapExpr interface {
	GetSpace() string
	// Resolve returns a valid Ref, which points to a Snapshot.
	Resolve(ctx context.Context, stx SpaceTx) (*gdat.Ref, error)
	isSnapExpr()
}

func ParseSnapExpr(x string) (SnapExpr, error) {
	if se, err := ParseSnap_Mark(x); err == nil {
		return se, nil
	}
	if se, err := ParseSnap_Exact(x); err == nil {
		return se, nil
	}
	return nil, fmt.Errorf("could not parse Snapshot expression from %q", x)
}

type SnapExpr_Exact struct {
	Space string
	Ref   gdat.Ref
}

func ParseSnap_Exact(x string) (*SnapExpr_Exact, error) {
	var spaceName string
	if i := strings.Index(x, ":"); i >= 0 {
		spaceName = x[:i]
		x = x[i+1:]
	}
	var ref gdat.Ref
	if err := ref.UnmarshalText([]byte(x)); err != nil {
		return nil, err
	}
	return &SnapExpr_Exact{
		Space: spaceName,
		Ref:   ref,
	}, nil
}

func (se *SnapExpr_Exact) isSnapExpr() {}

func (se *SnapExpr_Exact) GetSpace() string {
	return se.Space
}

func (se *SnapExpr_Exact) Resolve(ctx context.Context, tx SpaceTx) (*gdat.Ref, error) {
	return &se.Ref, nil
}

type SnapExpr_Mark struct {
	Space string
	Name  string
}

func ParseSnap_Mark(x string) (*SnapExpr_Mark, error) {
	var space string
	if i := strings.Index(x, ":"); i >= 0 {
		space = x[:i]
		x = x[i+1:]
	}
	return &SnapExpr_Mark{Space: space, Name: x}, nil
}

func (se SnapExpr_Mark) isSnapExpr() {}

func (se SnapExpr_Mark) GetSpace() string {
	return se.Space
}

func (se SnapExpr_Mark) Resolve(ctx context.Context, tx SpaceTx) (*gdat.Ref, error) {
	var ref gdat.Ref
	if ok, err := tx.GetTarget(ctx, se.Name, &ref); err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}
	return &ref, nil
}

type SnapExpr_Offset struct {
	X      SnapExpr
	Offset uint
}

func (se SnapExpr_Offset) isSnapExpr() {}
