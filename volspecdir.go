package got

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"os"
	"strings"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/got/pkg/fs"
	"github.com/brendoncarroll/got/pkg/volumes"
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
		if fs.IsNotExist(err) {
			return nil, volumes.ErrNotExist
		}
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
