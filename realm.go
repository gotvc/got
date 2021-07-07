package got

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/got/pkg/branches"
	"github.com/brendoncarroll/got/pkg/fs"
)

var _ Realm = &branchSpecDir{}

type cellFactory = func(spec CellSpec) (Cell, error)
type storeFactory = func(spec StoreSpec) (Store, error)

type branchSpecDir struct {
	makeDefault func() VolumeSpec
	cf          cellFactory
	sf          storeFactory
	fs          fs.FS
}

func newBranchSpecDir(makeDefault func() VolumeSpec, cf cellFactory, sf storeFactory, fs fs.FS) *branchSpecDir {
	return &branchSpecDir{
		makeDefault: makeDefault,
		cf:          cf,
		sf:          sf,
		fs:          fs,
	}
}

func (r *branchSpecDir) ForEach(ctx context.Context, fn func(string) error) error {
	return fs.WalkFiles(ctx, r.fs, "", func(p string) error {
		return fn(p)
	})
}

func (r *branchSpecDir) Create(ctx context.Context, name string) error {
	return r.CreateWithSpec(name, BranchSpec{
		Volume: r.makeDefault(),
	})
}

func (r *branchSpecDir) CreateWithSpec(name string, spec BranchSpec) error {
	if err := branches.CheckName(name); err != nil {
		return err
	}
	_, err := r.cf(spec.Volume.Cell)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(spec, "", " ")
	if err != nil {
		return err
	}
	return r.fs.WriteFile(name, bytes.NewReader(data))
}

func (r *branchSpecDir) Delete(ctx context.Context, k string) error {
	return r.fs.Remove(k)
}

func (r *branchSpecDir) Get(ctx context.Context, k string) (*Branch, error) {
	data, err := fs.ReadFile(r.fs, k)
	if err != nil {
		if fs.IsNotExist(err) {
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
	return &Branch{Volume: *vol}, nil
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
