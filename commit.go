package got

import (
	"context"
	"encoding/json"

	"github.com/brendoncarroll/got/pkg/gotfs"
	"github.com/brendoncarroll/got/pkg/refs"
	"github.com/pkg/errors"
)

type Commit struct {
	Version uint `json:"version"`
	Parent  *Ref `json:"parent"`
	Root    Root `json:"root"`
}

func PostCommit(ctx context.Context, s Store, x Commit) (*Ref, error) {
	return refs.Post(ctx, s, marshalCommit(x))
}

func GetCommit(ctx context.Context, s Store, ref Ref) (*Commit, error) {
	var x *Commit
	if err := refs.GetF(ctx, s, ref, func(data []byte) error {
		var err error
		x, err = parseCommit(data)
		return err
	}); err != nil {
		return nil, err
	}
	return x, nil
}

func Squash(ctx context.Context, s Store, xs []Commit) (*Commit, error) {
	if len(xs) < 1 {
		return nil, errors.Errorf("cannot squash 0 commits")
	}
	roots := make([]Root, len(xs))
	for i := range xs {
		roots[i] = xs[i].Root
	}
	root, err := gotfs.Merge(ctx, s, roots)
	if err != nil {
		return nil, err
	}
	return &Commit{
		Parent: xs[0].Parent,
		Root:   *root,
	}, nil
}

func Rebase(ctx context.Context, s Store, xs []Commit, onto Commit) ([]Commit, error) {
	var ys []Commit
	for i := range xs {
		if i > 0 {
			onto = xs[i-1]
		}
		y, err := RebaseOne(ctx, s, xs[i], onto)
		if err != nil {
			return nil, err
		}
		ys[i] = *y
	}
	return ys, nil
}

func RebaseOne(ctx context.Context, s Store, x Commit, onto Commit) (*Commit, error) {
	panic("not implemented")
}

// HasAncestor returns whether x has a as an ancestor
func HasAncestor(ctx context.Context, s Store, x, a Ref) (bool, error) {
	if refs.Equal(x, a) {
		return true, nil
	}
	commit, err := GetCommit(ctx, s, x)
	if err != nil {
		return false, err
	}
	if commit.Parent == nil {
		return false, nil
	}
	return HasAncestor(ctx, s, *commit.Parent, a)
}

func marshalCommit(c Commit) []byte {
	data, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	return data
}

func parseCommit(data []byte) (*Commit, error) {
	var commit Commit
	if err := json.Unmarshal(data, &commit); err != nil {
		return nil, err
	}
	return &commit, nil
}
