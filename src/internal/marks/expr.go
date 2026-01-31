package marks

import (
	"context"
	"strings"

	"github.com/gotvc/got/src/gdat"
)

type SnapExpr interface {
	GetSpace() string
	Resolve(ctx context.Context, stx SpaceTx) (*gdat.Ref, error)
	isSnapExpr()
}

func ParseSnapExpr(x string) (SnapExpr, error) {
	return ParseSnap_Exact(x)
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
