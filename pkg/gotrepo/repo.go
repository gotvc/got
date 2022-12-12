package gotrepo

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/exp/slog"

	"github.com/brendoncarroll/stdctx/logctx"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/cells"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotiam"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/gotvc/got/pkg/gotnet"
	"github.com/gotvc/got/pkg/gotvc"
	"github.com/gotvc/got/pkg/staging"
	"github.com/gotvc/got/pkg/stores"
)

const (
	MaxBlobSize = gotfs.DefaultMaxBlobSize
	MaxCellSize = 1 << 16
)

const (
	bucketDefault      = "default"
	bucketCellData     = "cells"
	bucketStaging      = "staging"
	bucketPorter       = "porter"
	bucketImportStores = "import_stores"
	bucketImportCaches = "import_caches"

	keyActive  = "ACTIVE"
	nameMaster = "master"
)

// fs paths
const (
	gotPrefix      = ".got"
	configPath     = ".got/config"
	privateKeyPath = ".got/private.pem"
	specDirPath    = ".got/branches"
	policyPath     = ".got/policy"
	dbPath         = ".got/got.db"
	blobsPath      = ".got/blobs"
	storeDBPath    = ".got/stores.db"
)

type (
	FS = posixfs.FS

	Cell   = cells.Cell
	Space  = branches.Space
	Volume = branches.Volume
	Branch = branches.Branch
	Store  = cadata.Store

	Ref  = gotkv.Ref
	Root = gotfs.Root

	Snap = gotvc.Snap
)

type Repo struct {
	rootPath     string
	repoFS       FS // repoFS is the directory that the repo is in
	db, storesDB *bolt.DB
	config       Config
	privateKey   inet256.PrivateKey
	// ctx is used as the background context for serving the repo
	ctx context.Context

	workingDir FS // workingDir is repoFS with reserved paths filtered.
	stage      *staging.Stage

	specDir   *branchSpecDir
	space     branches.Space
	iamEngine *iamEngine

	cellManager  *cellManager
	storeManager *storeManager
	gotNet       *gotnet.Service
}

func Init(p string) error {
	repoDirFS := posixfs.NewDirFS(p)
	if _, err := repoDirFS.Stat(configPath); posixfs.IsErrNotExist(err) {
	} else if err != nil {
		return err
	} else {
		return errors.Errorf("repo already exists")
	}
	if err := repoDirFS.Mkdir(gotPrefix, 0o755); err != nil {
		return err
	}
	// branches
	if err := repoDirFS.Mkdir(specDirPath, 0o755); err != nil {
		return err
	}
	// config
	config := DefaultConfig()
	if err := SaveConfig(repoDirFS, configPath, config); err != nil {
		return err
	}
	privKey := generatePrivateKey()
	if err := SavePrivateKey(repoDirFS, privateKeyPath, privKey); err != nil {
		return err
	}
	// iam
	if err := writeIfNotExists(repoDirFS, policyPath, 0o644, bytes.NewReader(nil)); err != nil {
		return err
	}
	// stores
	if err := repoDirFS.Mkdir(blobsPath, 0o755); err != nil {
		return err
	}
	r, err := Open(p)
	if err != nil {
		return err
	}
	return r.Close()
}

func Open(p string) (*Repo, error) {
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(os.Stderr))
	ctx = logctx.NewContext(ctx, &log)

	repoFS := posixfs.NewDirFS(p)
	config, err := LoadConfig(repoFS, configPath)
	if err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(p, dbPath), 0o644, &bolt.Options{
		Timeout: time.Second,
	})
	if err != nil {
		return nil, err
	}
	storesDB, err := bolt.Open(filepath.Join(p, storeDBPath), 0o644, &bolt.Options{
		Timeout: time.Second,
		NoSync:  true,
	})
	if err != nil {
		return nil, err
	}
	privateKey, err := LoadPrivateKey(repoFS, privateKeyPath)
	if err != nil {
		return nil, err
	}
	fsStore := stores.NewFSStore(posixfs.NewDirFS(filepath.Join(p, blobsPath)), MaxBlobSize)
	r := &Repo{
		rootPath:   p,
		repoFS:     repoFS,
		config:     *config,
		privateKey: privateKey,
		db:         db,
		storesDB:   storesDB,
		ctx:        ctx,

		workingDir: posixfs.NewFiltered(repoFS, func(x string) bool {
			return !strings.HasPrefix(x, gotPrefix)
		}),
		storeManager: newStoreManager(fsStore, storesDB),
		stage:        staging.New(newBoltKVStore(db, bucketStaging)),
	}
	r.cellManager = newCellManager(db, []string{bucketCellData})
	if r.iamEngine, err = newIAMEngine(r.repoFS); err != nil {
		return nil, err
	}
	r.specDir = newBranchSpecDir(r.makeDefaultVolume, r.MakeCell, r.MakeStore, posixfs.NewDirFS(filepath.Join(r.rootPath, specDirPath)))
	if r.space, err = r.spaceFromSpecs(r.config.Spaces); err != nil {
		return nil, err
	}
	if _, err := branches.CreateIfNotExists(ctx, r.specDir, nameMaster, branches.NewMetadata(false)); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Repo) Close() error {
	var errs []error
	for _, fn := range []func() error{
		r.db.Sync,
		r.db.Close,
		r.storesDB.Sync,
		r.storesDB.Close,
	} {
		if err := fn(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors while closing: %v", errs)
	}
	return nil
}

func (r *Repo) WorkingDir() FS {
	return r.workingDir
}

func (r *Repo) GetSpace() Space {
	return r.space
}

func (r *Repo) UpdateIAMPolicy(fn func(gotiam.Policy) gotiam.Policy) error {
	return r.iamEngine.Update(fn)
}

func (r *Repo) GetIAMPolicy() gotiam.Policy {
	return r.iamEngine.GetPolicy()
}

func (r *Repo) getFSOp(b *branches.Branch) *gotfs.Operator {
	return branches.NewGotFS(b)
}

func (r *Repo) getVCOp(b *branches.Branch) *gotvc.Operator {
	return branches.NewGotVC(b)
}

func (r *Repo) UnionStore() cadata.Store {
	return stores.AssertReadOnly(r.storeManager.store)
}

func dumpBucket(w io.Writer, b *bolt.Bucket) error {
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		fmt.Fprintf(w, "%q -> %q\n", k, v)
	}
	fmt.Fprintln(w)
	return nil
}

func (r *Repo) makeDefaultVolume(ctx context.Context) (VolumeSpec, error) {
	cellID, err := r.cellManager.Create(ctx)
	if err != nil {
		return VolumeSpec{}, err
	}
	cellSpec := CellSpec{
		Encrypted: &EncryptedCellSpec{
			Inner: CellSpec{
				Local: (*LocalCellSpec)(&cellID),
			},
			Secret: generateSecret(32),
		},
	}
	var storeIDs [3]uint64
	for i := range storeIDs {
		sid, err := r.storeManager.Create(ctx)
		if err != nil {
			return VolumeSpec{}, err
		}
		storeIDs[i] = sid
	}
	return VolumeSpec{
		Cell:     cellSpec,
		RawStore: StoreSpec{Local: (*LocalStoreSpec)(&storeIDs[0])},
		FSStore:  StoreSpec{Local: (*LocalStoreSpec)(&storeIDs[1])},
		VCStore:  StoreSpec{Local: (*LocalStoreSpec)(&storeIDs[2])},
	}, nil
}

func generateSecret(n int) []byte {
	x := make([]byte, n)
	if _, err := rand.Read(x); err != nil {
		panic(err)
	}
	return x
}

func bucketFromTx(tx *bolt.Tx, path []string) (*bolt.Bucket, error) {
	type bucketer interface {
		Bucket([]byte) *bolt.Bucket
		CreateBucketIfNotExists([]byte) (*bolt.Bucket, error)
	}
	getBucket := func(b bucketer, key string) (*bolt.Bucket, error) {
		if tx.Writable() {
			return b.CreateBucketIfNotExists([]byte(key))
		} else {
			return tx.Bucket([]byte(key)), nil
		}
	}
	b, err := getBucket(tx, path[0])
	if err != nil {
		return nil, err
	}
	if b == nil {
		return b, nil
	}
	path = path[1:]
	for len(path) > 0 {
		b, err = getBucket(b, path[0])
		if err != nil {
			return nil, err
		}
		if b == nil {
			return b, nil
		}
		path = path[1:]
	}
	return b, nil
}
