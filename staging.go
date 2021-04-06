package got

import (
	"context"
	"os"
	"path/filepath"
)

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
		return r.getFSOp().CreateFileFrom(ctx, s, x, p, rc)
	})
}

func (r *Repo) Remove(ctx context.Context, p string) error {
	return r.ApplyStaging(ctx, func(s Store, x Root) (*Root, error) {
		return r.getFSOp().RemoveAll(ctx, s, x, p)
	})
}
