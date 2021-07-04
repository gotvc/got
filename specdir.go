package got

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/got/pkg/branches"
	"github.com/brendoncarroll/got/pkg/fs"
)

var _ Realm = &branchSpecDir{}

type cellFactory = func(name string, spec CellSpec) (Cell, error)
type storeFactory = func(spec StoreSpec) (Store, error)

type branchSpecDir struct {
	cf cellFactory
	sf storeFactory
	fs fs.FS
}

func newBranchSpecDir(cf cellFactory, sf storeFactory, fs fs.FS) *branchSpecDir {
	return &branchSpecDir{
		cf: cf,
		sf: sf,
		fs: fs,
	}
}

func (r *branchSpecDir) ForEach(ctx context.Context, fn func(string) error) error {
	return fs.WalkFiles(ctx, r.fs, "", func(p string) error {
		return fn(p)
	})
}

func (r *branchSpecDir) Create(ctx context.Context, name string) error {
	newLocalSpec := func() *LocalStoreSpec {
		x := randomUint64()
		return &x
	}
	spec := VolumeSpec{
		Cell:     CellSpec{Local: &LocalCellSpec{}},
		VCStore:  StoreSpec{Local: newLocalSpec()},
		FSStore:  StoreSpec{Local: newLocalSpec()},
		RawStore: StoreSpec{Local: newLocalSpec()},
	}
	return r.CreateWithSpec(name, BranchSpec{Volume: spec})
}

func (csd *branchSpecDir) CreateWithSpec(name string, spec BranchSpec) error {
	_, err := csd.cf(name, spec.Volume.Cell)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(spec, "", " ")
	if err != nil {
		return err
	}
	return csd.fs.WriteFile(name, bytes.NewReader(data))
}

func (csd *branchSpecDir) Delete(ctx context.Context, k string) error {
	return csd.fs.Remove(k)
}

func (esd *branchSpecDir) Get(ctx context.Context, k string) (*Branch, error) {
	data, err := fs.ReadFile(esd.fs, k)
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
	return esd.makeBranch(k, spec)
}

func (esd *branchSpecDir) makeBranch(k string, spec BranchSpec) (*Branch, error) {
	vol, err := esd.makeVol(k, spec.Volume)
	if err != nil {
		return nil, err
	}
	return &Branch{Volume: vol}, nil
}

func (esd *branchSpecDir) makeVol(k string, spec VolumeSpec) (*Volume, error) {
	cell, err := esd.cf(k, spec.Cell)
	if err != nil {
		return nil, err
	}
	ss := [3]cadata.Store{}
	for i, spec := range []StoreSpec{
		spec.VCStore, spec.FSStore, spec.RawStore,
	} {
		ss[i], err = esd.sf(spec)
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

func randomUint64() uint64 {
	buf := [8]byte{}
	if _, err := rand.Read(buf[:]); err != nil {
		panic(err)
	}
	return binary.BigEndian.Uint64(buf[:])
}
