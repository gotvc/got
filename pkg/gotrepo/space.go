package gotrepo

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/brendoncarroll/go-tai64"
	"github.com/gotvc/got/pkg/branches"
)

var _ Space = &branchSpecDir{}

type cellFactory = func(spec CellSpec) (Cell, error)
type storeFactory = func(spec StoreSpec) (Store, error)

type branchSpecDir struct {
	makeDefault func() VolumeSpec
	cf          cellFactory
	sf          storeFactory
	fs          posixfs.FS
}

func newBranchSpecDir(makeDefault func() VolumeSpec, cf cellFactory, sf storeFactory, fs posixfs.FS) *branchSpecDir {
	return &branchSpecDir{
		makeDefault: makeDefault,
		cf:          cf,
		sf:          sf,
		fs:          fs,
	}
}

func (r *branchSpecDir) ForEach(ctx context.Context, span branches.Span, fn func(string) error) error {
	return posixfs.WalkLeaves(ctx, r.fs, "", func(p string, _ posixfs.DirEnt) error {
		return fn(p)
	})
}

func (r *branchSpecDir) Create(ctx context.Context, name string, params branches.Params) (*Branch, error) {
	return r.CreateWithSpec(name, BranchSpec{
		Volume:    r.makeDefault(),
		Salt:      params.Salt,
		CreatedAt: tai64.Now().TAI64(),
	})
}

func (r *branchSpecDir) CreateWithSpec(name string, spec BranchSpec) (*Branch, error) {
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

func (r *branchSpecDir) makeBranch(k string, spec BranchSpec) (*Branch, error) {
	vol, err := r.makeVol(k, spec.Volume)
	if err != nil {
		return nil, err
	}
	return &Branch{
		Volume: *vol,
		Salt:   spec.Salt,
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
