package gotrepo

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/fs"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotkv/ptree"
	"github.com/gotvc/got/pkg/stores"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

// default bucket
const (
	bucketDefault = "default"
	keyStaging    = "STAGING"
	keyActive     = "ACTIVE"
	nameMaster    = "master"
	MaxBlobSize   = 1 << 20
)

const (
	bucketCellData = "cells"
	bucketStores   = "stores"
	bucketTracker  = "tracker"
	bucketPorter   = "porter"
)

// fs paths
const (
	gotPrefix      = ".got"
	configPath     = ".got/config"
	privateKeyPath = ".got/private.pem"
	specDirPath    = ".got/branches"
	policyPath     = ".got/policy"
	storePath      = ".got/blobs"
)

type Repo struct {
	rootPath   string
	repoFS     FS // repoFS is the directory that the repo is in
	db         *bolt.DB
	config     Config
	policy     *Policy
	privateKey p2p.PrivateKey

	workingDir FS // workingDir is repoFS with reserved paths filtered.
	porter     *porter
	tracker    *tracker

	specDir *branchSpecDir
	realm   Space

	cellManager  *cellManager
	storeManager *storeManager
	dop          gdat.Operator
	fsop         gotfs.Operator
}

func Init(p string) error {
	repoDirFS := fs.NewDirFS(p)
	if _, err := repoDirFS.Stat(configPath); fs.IsErrNotExist(err) {
	} else if err != nil {
		return err
	} else {
		return errors.Errorf("repo already exists")
	}
	if err := repoDirFS.Mkdir(gotPrefix, 0o755); err != nil {
		return err
	}
	if err := repoDirFS.Mkdir(specDirPath, 0o755); err != nil {
		return err
	}
	if err := repoDirFS.Mkdir(storePath, 0o755); err != nil {
		return err
	}
	config := DefaultConfig()
	if err := SaveConfig(repoDirFS, configPath, config); err != nil {
		return err
	}
	privKey := generatePrivateKey()
	if err := SavePrivateKey(repoDirFS, privateKeyPath, privKey); err != nil {
		return err
	}
	if err := writeIfNotExists(repoDirFS, policyPath, 0o644, bytes.NewReader(nil)); err != nil {
		return err
	}
	r, err := Open(p)
	if err != nil {
		return err
	}
	return r.Close()
}

func Open(p string) (*Repo, error) {
	ctx := context.TODO()
	repoFS := fs.NewDirFS(p)
	config, err := LoadConfig(repoFS, configPath)
	if err != nil {
		return nil, err
	}
	db, err := bolt.Open(dbPath(p), 0o644, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	db.NoSync = true
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
		workingDir: fs.NewFiltered(repoFS, func(x string) bool {
			return !strings.HasPrefix(x, gotPrefix)
		}),
		tracker: newTracker(db, []string{bucketTracker}),
		dop:     gdat.NewOperator(),
		fsop:    gotfs.NewOperator(),
	}
	r.porter = newPorter(db, []string{bucketPorter}, r.getFSOp())
	fsStore := stores.NewFSStore(r.getSubFS(storePath), MaxBlobSize)
	r.storeManager = newStoreManager(fsStore, r.db, bucketStores)
	r.cellManager = newCellManager(db, []string{bucketCellData})

	r.specDir = newBranchSpecDir(r.makeDefaultVolume, r.MakeCell, r.MakeStore, fs.NewDirFS(filepath.Join(r.rootPath, specDirPath)))
	if err := branches.CreateIfNotExists(ctx, r.specDir, nameMaster); err != nil {
		return nil, err
	}
	r.realm, err = branches.NewMultiSpace([]branches.Layer{
		{Prefix: "", Target: r.specDir},
	})
	if err != nil {
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

func (r *Repo) GetSpace() Space {
	return r.realm
}

func (r *Repo) getSubFS(prefix string) fs.FS {
	return fs.NewPrefixed(r.repoFS, prefix)
}

func (r *Repo) getFSOp() *gotfs.Operator {
	return &r.fsop
}

func (r *Repo) getDataOp() *gdat.Operator {
	return &r.dop
}

func (r *Repo) UnionStore() cadata.Store {
	return stores.AssertReadOnly(r.storeManager.store)
}

func dbPath(x string) string {
	return filepath.Join(x, gotPrefix, "local.db")
}

func (r *Repo) DebugDB(ctx context.Context, w io.Writer) error {
	return r.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketDefault))
		if b != nil {
			fmt.Fprintf(w, "BUCKET: %s\n", bucketDefault)
			dumpBucket(w, b)
		}
		b = tx.Bucket([]byte(bucketCellData))
		if b != nil {
			fmt.Fprintf(w, "BUCKET: %s\n", bucketCellData)
			dumpBucket(w, b)
		}
		return nil
	})
}

func (r *Repo) DebugFS(ctx context.Context, w io.Writer) error {
	_, branch, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	vol := branch.Volume
	x, err := getSnapshot(ctx, vol.Cell)
	if err != nil {
		return err
	}
	if x == nil {
		return errors.Errorf("no snapshot, no root")
	}
	ptree.DebugTree(vol.FSStore, x.Root)
	return nil
}

func dumpBucket(w io.Writer, b *bolt.Bucket) {
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		fmt.Fprintf(w, "%q -> %q\n", k, v)
	}
	fmt.Fprintln(w)
}
