package got

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"

	"github.com/brendoncarroll/got/pkg/fs"
)

var _ Realm = &volSpecDir{}

type cellFactory = func(name string, spec CellSpec) (Cell, error)
type storeFactory = func(spec StoreSpec) (Store, error)

type volSpecDir struct {
	cf cellFactory
	sf storeFactory
	fs fs.FS
}

func newVolSpecDir(cf cellFactory, sf storeFactory, fs fs.FS) *volSpecDir {
	return &volSpecDir{
		cf: cf,
		sf: sf,
		fs: fs,
	}
}

func (esd *volSpecDir) List(ctx context.Context, prefix string) ([]string, error) {
	var ids []string
	err := esd.fs.ReadDir("", func(finfo os.FileInfo) error {
		name := finfo.Name()
		if !strings.HasPrefix(name, prefix) {
			return nil
		}
		ids = append(ids, name)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func (csd *volSpecDir) Create(ctx context.Context, name string) error {
	spec := VolumeSpec{
		Cell:  CellSpec{Local: &LocalCellSpec{}},
		Store: StoreSpec{Local: &LocalStoreSpec{}},
	}
	return csd.CreateEnvFromSpec(name, spec)
}

func (csd *volSpecDir) CreateEnvFromSpec(name string, spec VolumeSpec) error {
	_, err := csd.cf(name, spec.Cell)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(spec, "", " ")
	if err != nil {
		return err
	}
	return csd.fs.WriteFile(name, bytes.NewReader(data))
}

func (csd *volSpecDir) Delete(ctx context.Context, k string) error {
	return csd.fs.Remove(k)
}

func (esd *volSpecDir) Get(ctx context.Context, k string) (*Volume, error) {
	data, err := fs.ReadFile(esd.fs, k)
	if err != nil {
		return nil, err
	}
	spec := VolumeSpec{}
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, err
	}
	return esd.makeEnv(k, spec)
}

func (esd *volSpecDir) makeEnv(k string, spec VolumeSpec) (*Volume, error) {
	cell, err := esd.cf(k, spec.Cell)
	if err != nil {
		return nil, err
	}
	store, err := esd.sf(spec.Store)
	if err != nil {
		return nil, err
	}
	return &Volume{Cell: cell, Store: store}, nil
}
