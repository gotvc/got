package got

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"

	"github.com/brendoncarroll/got/pkg/fs"
)

var _ Realm = &envSpecDir{}

type cellFactory = func(name string, spec CellSpec) (Cell, error)
type storeFactory = func(spec StoreSpec) (Store, error)

type envSpecDir struct {
	cf cellFactory
	sf storeFactory
	fs fs.FS
}

func newEnvSpecDir(cf cellFactory, sf storeFactory, fs fs.FS) *envSpecDir {
	return &envSpecDir{
		cf: cf,
		sf: sf,
		fs: fs,
	}
}

func (esd *envSpecDir) List(ctx context.Context, prefix string) ([]string, error) {
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

func (csd *envSpecDir) Create(ctx context.Context, name string) error {
	spec := EnvSpec{
		Cell:  CellSpec{Local: &LocalCellSpec{}},
		Store: StoreSpec{Local: &LocalStoreSpec{}},
	}
	return csd.CreateEnvFromSpec(name, spec)
}

func (csd *envSpecDir) CreateEnvFromSpec(name string, spec EnvSpec) error {
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

func (csd *envSpecDir) Delete(ctx context.Context, k string) error {
	return csd.fs.Remove(k)
}

func (esd *envSpecDir) Get(ctx context.Context, k string) (*Env, error) {
	data, err := fs.ReadFile(esd.fs, k)
	if err != nil {
		return nil, err
	}
	spec := EnvSpec{}
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, err
	}
	return esd.makeEnv(k, spec)
}

func (esd *envSpecDir) makeEnv(k string, spec EnvSpec) (*Env, error) {
	cell, err := esd.cf(k, spec.Cell)
	if err != nil {
		return nil, err
	}
	store, err := esd.sf(spec.Store)
	if err != nil {
		return nil, err
	}
	return &Env{Cell: cell, Store: store}, nil
}
