package got

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/got/pkg/fs"
	"github.com/brendoncarroll/got/pkg/gotfs"
	"github.com/brendoncarroll/got/pkg/gotnet"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

const (
	bucketDefault = "default"
	keyStaging    = "STAGING"
	keyActive     = "ACTIVE"
	nameMaster    = "master"

	gotPrefix      = ".got"
	configPath     = ".got/config"
	privateKeyPath = ".got/private.pem"
	cellSpecPath   = ".got/cell_specs"
	policyPath     = ".got/policy"
)

type Repo struct {
	rootPath   string
	repoFS     FS
	db         *bolt.DB
	config     Config
	policy     *Policy
	privateKey p2p.PrivateKey

	store      blobs.Store
	cellSpaces []CellSpace
	workingDir FS
}

func InitRepo(p string) error {
	gotDir := filepath.Join(p, gotPrefix)
	if err := os.Mkdir(gotDir, 0o755); err != nil {
		return err
	}
	if err := os.Mkdir(cellSpecPath, 0o755); err != nil {
		return err
	}
	repoDirFS := fs.NewDirFS(p)
	if _, err := repoDirFS.Stat(configPath); os.IsNotExist(err) {
	} else if err != nil {
		return err
	} else {
		return errors.Errorf("repo already exists")
	}
	config := DefaultConfig()
	if err := SaveConfig(repoDirFS, configPath, config); err != nil {
		return err
	}
	privKey := generatePrivateKey()
	if err := SavePrivateKey(repoDirFS, privateKeyPath, privKey); err != nil {
		return err
	}
	if err := fs.WriteIfNotExists(repoDirFS, policyPath, nil); err != nil {
		return err
	}
	r, err := OpenRepo(p)
	if err != nil {
		return err
	}
	return r.Close()
}

func OpenRepo(p string) (*Repo, error) {
	repoFS := fs.NewDirFS(p)
	config, err := LoadConfig(repoFS, configPath)
	if err != nil {
		return nil, err
	}
	db, err := bolt.Open(dbPath(p), 0o644, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	privateKey, err := LoadPrivateKey(repoFS, privateKeyPath)
	if err != nil {
		return nil, err
	}
	r := &Repo{
		rootPath:   p,
		repoFS:     repoFS,
		config:     *config,
		privateKey: privateKey,
		db:         db,
		workingDir: fs.NewFilterFS(repoFS, func(x string) bool {
			return !strings.HasPrefix(x, gotPrefix)
		}),
	}
	if _, err := r.GetCellSpace().Get(context.TODO(), nameMaster); os.IsNotExist(err) {
		spec := CellSpec{Local: &LocalCellSpec{}}
		if err := r.CreateCell("master", spec); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Repo) Close() (retErr error) {
	for _, fn := range []func() error{
		r.db.Close,
	} {
		if err := fn(); retErr == nil {
			retErr = err
		}
	}
	return retErr
}

func (r *Repo) WorkingDir() FS {
	return r.workingDir
}

func (r *Repo) ApplyStaging(ctx context.Context, fn func(x Ref) (*Ref, error)) error {
	return boltApply(r.db, bucketDefault, []byte(keyStaging), func(x []byte) ([]byte, error) {
		var xRef *Ref
		var err error
		if len(x) < 1 {
			xRef, err = gotfs.New(ctx, r.GetStore())
			if err != nil {
				return nil, err
			}
		} else {
			xRef = &Ref{}
			if err := json.Unmarshal(x, xRef); err != nil {
				return nil, err
			}
		}
		yRef, err := fn(*xRef)
		if err != nil {
			return nil, err
		}
		return json.Marshal(yRef)
	})
}

func (r *Repo) GetACL() gotnet.ACL {
	return r.policy
}

func (r *Repo) GetStore() Store {
	return r.store
}

func dbPath(x string) string {
	return filepath.Join(x, gotPrefix, "local.db")
}
