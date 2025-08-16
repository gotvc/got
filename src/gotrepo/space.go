package gotrepo

import (
	"bytes"
	"context"
	"encoding/json"

	"go.brendoncarroll.net/state"
	"go.brendoncarroll.net/state/posixfs"
	"go.brendoncarroll.net/tai64"

	"github.com/gotvc/got/src/branches"
)

var _ Space = &branchSpecDir{}

type branchSpecDir struct {
	makeDefault func(context.Context) (VolumeSpec, error)
	mkvol       func(ctx context.Context, name string, spec VolumeSpec) (Volume, error)
	fs          posixfs.FS
}

func newBranchSpecDir(makeDefault func(ctx context.Context) (VolumeSpec, error), mkvol func(ctx context.Context, name string, spec VolumeSpec) (Volume, error), fs posixfs.FS) *branchSpecDir {
	return &branchSpecDir{
		makeDefault: makeDefault,
		mkvol:       mkvol,
		fs:          fs,
	}
}

func (r *branchSpecDir) List(ctx context.Context, span branches.Span, limit int) (ret []string, _ error) {
	err := posixfs.WalkLeavesSpan(ctx, r.fs, "", state.Span[string]{}, func(p string, _ posixfs.DirEnt) error {
		if span.Contains(p) {
			ret = append(ret, p)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (r *branchSpecDir) Create(ctx context.Context, name string, params branches.Config) (*branches.Info, error) {
	vspec, err := r.makeDefault(ctx)
	if err != nil {
		return nil, err
	}
	return r.CreateWithSpec(ctx, name, BranchSpec{
		Volume: vspec,
		Info: branches.Info{
			Salt:        params.Salt,
			CreatedAt:   tai64.Now().TAI64(),
			Annotations: params.Annotations,
		},
	})
}

func (r *branchSpecDir) CreateWithSpec(ctx context.Context, name string, spec BranchSpec) (*branches.Info, error) {
	if err := branches.CheckName(name); err != nil {
		return nil, err
	}
	_, err := r.mkvol(ctx, name, spec.Volume)
	if err != nil {
		return nil, err
	}
	data, err := json.MarshalIndent(spec, "", " ")
	if err != nil {
		return nil, err
	}
	if err := writeIfNotExists(r.fs, name, 0o644, bytes.NewReader(data)); err != nil {
		return nil, err
	}
	return r.Get(ctx, name)
}

func (r *branchSpecDir) Delete(ctx context.Context, k string) error {
	return r.fs.Remove(k)
}

func (r *branchSpecDir) Get(ctx context.Context, k string) (*branches.Info, error) {
	spec, err := r.loadSpec(ctx, k)
	if err != nil {
		return nil, err
	}
	return &spec.Info, nil
}

func (r *branchSpecDir) Set(ctx context.Context, k string, cfg branches.Config) error {
	spec, err := r.loadSpec(ctx, k)
	if err != nil {
		return err
	}
	spec.Mode = cfg.Mode
	spec.Salt = cfg.Salt
	spec.Annotations = cfg.Annotations
	return r.saveSpec(ctx, k, *spec)
}

func (r *branchSpecDir) Open(ctx context.Context, name string) (branches.Volume, error) {
	spec, err := r.loadSpec(ctx, name)
	if err != nil {
		return nil, err
	}
	return r.mkvol(ctx, name, spec.Volume)
}

func (r *branchSpecDir) loadSpec(ctx context.Context, k string) (*BranchSpec, error) {
	data, err := posixfs.ReadFile(ctx, r.fs, k)
	if err != nil {
		if posixfs.IsErrNotExist(err) {
			return nil, branches.ErrNotExist
		}
		return nil, err
	}
	spec := BranchSpec{}
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, err
	}
	return &spec, nil
}

func (r *branchSpecDir) saveSpec(ctx context.Context, k string, spec BranchSpec) error {
	data, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	return posixfs.PutFile(ctx, r.fs, k, 0o644, bytes.NewReader(data))
}
