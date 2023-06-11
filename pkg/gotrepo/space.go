package gotrepo

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/brendoncarroll/go-tai64"

	"github.com/gotvc/got/pkg/branches"
)

var _ Space = &branchSpecDir{}

type cellFactory = func(spec CellSpec) (Cell, error)
type storeFactory = func(spec StoreSpec) (Store, error)

type branchSpecDir struct {
	makeDefault func(context.Context) (VolumeSpec, error)
	cf          cellFactory
	sf          storeFactory
	fs          posixfs.FS
}

func newBranchSpecDir(makeDefault func(ctx context.Context) (VolumeSpec, error), cf cellFactory, sf storeFactory, fs posixfs.FS) *branchSpecDir {
	return &branchSpecDir{
		makeDefault: makeDefault,
		cf:          cf,
		sf:          sf,
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

func (r *branchSpecDir) Create(ctx context.Context, name string, params branches.Metadata) (*Branch, error) {
	vspec, err := r.makeDefault(ctx)
	if err != nil {
		return nil, err
	}
	return r.CreateWithSpec(ctx, name, BranchSpec{
		Volume:      vspec,
		Salt:        params.Salt,
		CreatedAt:   tai64.Now().TAI64(),
		Annotations: params.Annotations,
	})
}

func (r *branchSpecDir) CreateWithSpec(ctx context.Context, name string, spec BranchSpec) (*Branch, error) {
	if err := branches.CheckName(name); err != nil {
		return nil, err
	}
	_, err := r.cf(spec.Volume.Cell)
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
	return r.Get(context.TODO(), name)
}

func (r *branchSpecDir) Delete(ctx context.Context, k string) error {
	return r.fs.Remove(k)
}

func (r *branchSpecDir) Get(ctx context.Context, k string) (*Branch, error) {
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
	return r.makeBranch(k, spec)
}

func (r *branchSpecDir) Set(ctx context.Context, k string, md branches.Metadata) error {
	data, err := posixfs.ReadFile(ctx, r.fs, k)
	if err != nil {
		if posixfs.IsErrNotExist(err) {
			return branches.ErrNotExist
		}
		return err
	}
	spec := BranchSpec{}
	if err := json.Unmarshal(data, &spec); err != nil {
		return err
	}
	spec.Salt = md.Salt
	spec.Annotations = md.Annotations
	data, err = json.Marshal(spec)
	if err != nil {
		return err
	}
	return posixfs.PutFile(ctx, r.fs, k, 0o644, bytes.NewReader(data))
}

func (r *branchSpecDir) makeBranch(k string, spec BranchSpec) (*Branch, error) {
	vol, err := r.makeVol(k, spec.Volume)
	if err != nil {
		return nil, err
	}
	return &Branch{
		Volume: *vol,
		Metadata: branches.Metadata{
			Salt:        spec.Salt,
			Annotations: spec.Annotations,
		},
		CreatedAt: spec.CreatedAt,
	}, nil
}

func (r *branchSpecDir) makeVol(k string, spec VolumeSpec) (*Volume, error) {
	cell, err := r.cf(spec.Cell)
	if err != nil {
		return nil, err
	}
	ss := [3]cadata.Store{}
	for i, spec := range []StoreSpec{
		spec.VCStore, spec.FSStore, spec.RawStore,
	} {
		ss[i], err = r.sf(spec)
		if err != nil {
			return nil, err
		}
	}
	return &Volume{
		Cell:     cell,
		VCStore:  ss[0],
		FSStore:  ss[1],
		RawStore: ss[2],
	}, nil
}
