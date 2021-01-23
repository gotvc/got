package got

import (
	"context"
	"encoding/json"
	"os"
	"strings"

	"github.com/brendoncarroll/got/pkg/fs"
)

var _ CellSpace = &cellSpecDir{}

type cellSpecDir struct {
	r  *Repo
	fs fs.FS
}

func newCellSpecDir(r *Repo, fs fs.FS) *cellSpecDir {
	return &cellSpecDir{
		r:  r,
		fs: fs,
	}
}

func (csd *cellSpecDir) ForEach(ctx context.Context, prefix string, fn func(k string) error) error {
	return csd.fs.ReadDir("", func(finfo os.FileInfo) error {
		name := finfo.Name()
		if !strings.HasPrefix(name, prefix) {
			return nil
		}
		return fn(name)
	})
}

func (csd *cellSpecDir) Get(ctx context.Context, k string) (Cell, error) {
	data, err := fs.ReadFile(csd.fs, k)
	if err != nil {
		return nil, err
	}
	spec := CellSpec{}
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, err
	}
	return csd.r.MakeCell(k, spec)
}
