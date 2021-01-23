package got

import (
	"context"
	"encoding/json"

	"github.com/brendoncarroll/got/pkg/gotfs"
	"github.com/brendoncarroll/got/pkg/gotkv"
	"github.com/pkg/errors"
)

type Ref = gotkv.Ref

type Commit struct {
	Version uint
	Parent  *Ref
	Root    Ref
}

func PostCommit(ctx context.Context, s Store, x Commit) (*Ref, error) {
	data, err := json.Marshal(x)
	if err != nil {
		return nil, err
	}
	return gotkv.PostRaw(ctx, s, data)
}

func GetCommit(ctx context.Context, s Store, ref Ref) (*Commit, error) {
	x := &Commit{}
	if err := gotkv.GetRawF(ctx, s, ref, func(data []byte) error {
		return json.Unmarshal(data, x)
	}); err != nil {
		return nil, err
	}
	return x, nil
}

func Squash(ctx context.Context, s Store, xs []Commit) (*Commit, error) {
	if len(xs) < 1 {
		return nil, errors.Errorf("cannot squash 0 commits")
	}
	refs := make([]Ref, len(xs))
	for i := range xs {
		refs[i] = xs[i].Root
	}
	root, err := gotfs.Merge(ctx, s, refs)
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
		y, err := ReplaceParent(ctx, s, xs[i], onto)
		if err != nil {
			return nil, err
		}
		ys[i] = *y
	}
	return ys, nil
}

func ReplaceParent(ctx context.Context, s Store, x Commit, newParent Commit) (*Commit, error) {
	panic("")
}
