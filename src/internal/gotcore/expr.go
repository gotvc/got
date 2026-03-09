package gotcore

import (
	"context"
	"fmt"
	"strings"

	"github.com/gotvc/got/src/gdat"
)

// CommitExpr is a sum type representing the different ways
// to refer to a Commit in Got
// So far, there are 2 primitive ways
// - Exactly by Ref
// - By Mark
// And 1 higher-order way
// - By an offset from the result of a previous expression
type CommitExpr interface {
	GetSpace() string
	// Resolve returns a valid Ref, which points to a Commit.
	Resolve(ctx context.Context, stx SpaceTx) (*gdat.Ref, error)
	isSnapExpr()
}

func ParseCommitExpr(x string) (CommitExpr, error) {
	if se, err := ParseCommit_Mark(x); err == nil {
		return se, nil
	}
	if se, err := ParseCommit_Exact(x); err == nil {
		return se, nil
	}
	return nil, fmt.Errorf("could not parse Commit expression from %q", x)
}

type CommitExpr_Exact struct {
	Space string
	Ref   gdat.Ref
}

func ParseCommit_Exact(x string) (*CommitExpr_Exact, error) {
	var spaceName string
	if i := strings.Index(x, ":"); i >= 0 {
		spaceName = x[:i]
		x = x[i+1:]
	}
	var ref gdat.Ref
	if err := ref.UnmarshalText([]byte(x)); err != nil {
		return nil, err
	}
	return &CommitExpr_Exact{
		Space: spaceName,
		Ref:   ref,
	}, nil
}

func (se *CommitExpr_Exact) isSnapExpr() {}

func (se *CommitExpr_Exact) GetSpace() string {
	return se.Space
}

func (se *CommitExpr_Exact) Resolve(ctx context.Context, tx SpaceTx) (*gdat.Ref, error) {
	return &se.Ref, nil
}

type CommitExpr_Mark struct {
	Space string
	Name  string
}

func ParseCommit_Mark(x string) (*CommitExpr_Mark, error) {
	var space string
	if i := strings.Index(x, ":"); i >= 0 {
		space = x[:i]
		x = x[i+1:]
	}
	return &CommitExpr_Mark{Space: space, Name: x}, nil
}

func (se CommitExpr_Mark) isSnapExpr() {}

func (se CommitExpr_Mark) GetSpace() string {
	return se.Space
}

func (se CommitExpr_Mark) Resolve(ctx context.Context, tx SpaceTx) (*gdat.Ref, error) {
	var ref gdat.Ref
	if ok, err := tx.GetTarget(ctx, se.Name, &ref); err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}
	return &ref, nil
}

func (se CommitExpr_Mark) String() string {
	return se.Space + ":" + se.Name
}

type CommitExpr_Offset struct {
	X      CommitExpr
	Offset uint
}

func (se CommitExpr_Offset) isSnapExpr() {}
