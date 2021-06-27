package got

import (
	"context"
	"encoding/json"
	"time"

	"github.com/brendoncarroll/got/pkg/fs"
	"github.com/brendoncarroll/got/pkg/gotfs"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

type porter struct {
	db         *bolt.DB
	bucketPath []string
	fsop       *gotfs.Operator
}

func newPorter(db *bolt.DB, bucketPath []string, fsop *gotfs.Operator) *porter {
	return &porter{
		db:         db,
		fsop:       fsop,
		bucketPath: bucketPath,
	}
}

// Import imports the path fsx into vol, if the modified time has changed.
func (p *porter) Import(ctx context.Context, ms, ds Store, fsx fs.FS, pth string) (*gotfs.Root, error) {
	if yes, err := p.needsImport(ctx, fsx, pth); err != nil {
		return nil, err
	} else if !yes {
		ce, err := p.getCacheEntry(pth)
		if err != nil {
			return nil, err
		}
		return &ce.Root, nil
	}
	stat, err := fsx.Stat(pth)
	if err != nil {
		return nil, err
	}
	if !stat.Mode().IsRegular() {
		return nil, errors.Errorf("porter cannot import non-regular file at path %q", pth)
	}
	f, err := fsx.Open(pth)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	root, err := p.fsop.CreateFileRoot(ctx, ms, ds, f)
	if err != nil {
		return nil, err
	}
	if err := p.updateCacheEntry(pth, &cacheEntry{
		Root:       *root,
		ModifiedAt: stat.ModTime(),
	}); err != nil {
		return nil, err
	}
	return root, nil
}

func (p *porter) Export(ctx context.Context, ms, ds Store, root gotfs.Root, fsx fs.FS, pth string) error {
	if yes, err := p.needsExport(ctx, fsx, pth, root); err != nil {
		return err
	} else if !yes {
		return nil
	}
	r := p.fsop.NewReader(ctx, ms, ds, root, pth)
	if err := fsx.WriteFile(pth, r); err != nil {
		return err
	}
	finfo, err := fsx.Stat(pth)
	if err != nil {
		return err
	}
	return p.updateCacheEntry(pth, &cacheEntry{
		ModifiedAt: finfo.ModTime(),
		Root:       root,
	})
}

func (p *porter) Cleanup(ctx context.Context) error {
	return nil
}

func (p *porter) needsImport(ctx context.Context, fsx fs.FS, pth string) (bool, error) {
	finfo, err := fsx.Stat(pth)
	if err != nil {
		return false, err
	}
	ce, err := p.getCacheEntry(pth)
	if err != nil {
		return false, err
	}
	return ce == nil || ce.ModifiedAt != finfo.ModTime(), nil
}

func (p *porter) needsExport(ctx context.Context, fsx fs.FS, pth string, root gotfs.Root) (bool, error) {
	finfo, err := fsx.Stat(pth)
	if err != nil {
		return false, err
	}
	ce, err := p.getCacheEntry(pth)
	if err != nil {
		return false, err
	}
	if ce.ModifiedAt != finfo.ModTime() {
		return true, nil
	}
	return gotfs.Equal(ce.Root, root), nil
}

type cacheEntry struct {
	ModifiedAt time.Time  `json:"mt"`
	Root       gotfs.Root `json:"r"`
}

func (p *porter) updateCacheEntry(pth string, ce *cacheEntry) error {
	return p.db.Batch(func(tx *bolt.Tx) error {
		b, err := bucketFromTx(tx, p.bucketPath)
		if err != nil {
			return err
		}
		key := []byte(pth)
		v := b.Get(key)
		if v != nil {
			var current cacheEntry
			if err := json.Unmarshal(v, &current); err != nil {
				return err
			}
			if ce.ModifiedAt.Before(current.ModifiedAt) {
				return errors.Errorf("new mtime is before current mtime")
			}
		}
		data, err := json.Marshal(ce)
		if err != nil {
			return err
		}
		return b.Put(key, data)
	})
}

func (p *porter) getCacheEntry(pth string) (*cacheEntry, error) {
	var ce *cacheEntry
	err := p.db.View(func(tx *bolt.Tx) error {
		b, err := bucketFromTx(tx, p.bucketPath)
		if err != nil {
			return err
		}
		if b == nil {
			return nil
		}
		v := b.Get([]byte(pth))
		if v == nil {
			return nil
		}
		ce = &cacheEntry{}
		return json.Unmarshal(v, ce)
	})
	if err != nil {
		return nil, err
	}
	return ce, nil
}
