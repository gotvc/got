package got

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
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
	x := randomUint64()
	spec := VolumeSpec{
		Cell:  CellSpec{Local: &LocalCellSpec{}},
		Store: StoreSpec{Local: &LocalStoreSpec{ID: x}},
	}
	return csd.CreateWithSpec(name, spec)
}

func (csd *volSpecDir) CreateWithSpec(name string, spec VolumeSpec) error {
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
	return esd.makeVol(k, spec)
}

func (esd *volSpecDir) makeVol(k string, spec VolumeSpec) (*Volume, error) {
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

func randomUint64() uint64 {
	buf := [8]byte{}
	if _, err := rand.Read(buf[:]); err != nil {
		panic(err)
	}
	return binary.BigEndian.Uint64(buf[:])
}
