package got

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/brendoncarroll/got/pkg/cells"
	"github.com/brendoncarroll/got/pkg/gotvc"
)

func (r *Repo) ApplyStaging(ctx context.Context, fn func(s Store, x Root) (*gotvc.Root, error)) error {
	vol := r.GetStaging()
	store := vol.Store
	return cells.Apply(ctx, vol.Cell, func(data []byte) ([]byte, error) {
		var x Root
		var err error
		if len(data) > 0 {
			if err := json.Unmarshal(data, &x); err != nil {
				return nil, err
			}
		}
		yDelta, err := fn(store, x)
		if err != nil {
			return nil, err
		}
		return json.Marshal(yDelta)
	})
}

func (r *Repo) Add(ctx context.Context, p string) error {
	finfo, err := r.workingDir.Stat(p)
	if err != nil {
		return err
	}
	if finfo.IsDir() {
		return r.workingDir.ReadDir(p, func(finfo os.FileInfo) error {
			p2 := filepath.Join(p, finfo.Name())
			return r.Add(ctx, p2)
		})
	}
	return r.ApplyStaging(ctx, func(s Store, x Root) (*Root, error) {
		rc, err := r.workingDir.Open(p)
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		return r.getFSOp().CreateFile(ctx, s, x, p, rc)
	})
}

func (r *Repo) Remove(ctx context.Context, p string) error {
	return r.ApplyStaging(ctx, func(s Store, x Root) (*Root, error) {
		return r.getFSOp().RemoveAll(ctx, s, x, p)
	})
}
