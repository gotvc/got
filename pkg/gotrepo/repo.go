package gotrepo

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/cells"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/gotvc/got/pkg/gotnet"
	"github.com/gotvc/got/pkg/gotvc"
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
	MaxBlobSize   = gotfs.DefaultMaxBlobSize
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
	rootPath   string
	repoFS     FS // repoFS is the directory that the repo is in
	db         *bolt.DB
	config     Config
	policy     *Policy
	privateKey p2p.PrivateKey

	workingDir FS // workingDir is repoFS with reserved paths filtered.
	tracker    *tracker

	specDir *branchSpecDir
	space   Space

	cellManager  *cellManager
	storeManager *storeManager
	dop          gdat.Operator
	fsop         gotfs.Operator
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
	repoFS := posixfs.NewDirFS(p)
	config, err := LoadConfig(repoFS, configPath)
	if err != nil {
		return nil, err
	}
	db, err := bolt.Open(dbPath(p), 0o644, &bolt.Options{
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
	r := &Repo{
		rootPath:   p,
		repoFS:     repoFS,
		config:     *config,
		privateKey: privateKey,
		db:         db,
		workingDir: posixfs.NewFiltered(repoFS, func(x string) bool {
			return !strings.HasPrefix(x, gotPrefix)
		}),
		tracker: newTracker(db, []string{bucketTracker}),
		dop:     gdat.NewOperator(),
		fsop:    gotfs.NewOperator(),
	}
	fsStore := stores.NewFSStore(r.getSubFS(storePath), MaxBlobSize)
	r.storeManager = newStoreManager(fsStore, r.db, bucketStores)
	r.cellManager = newCellManager(db, []string{bucketCellData})

	r.specDir = newBranchSpecDir(r.makeDefaultVolume, r.MakeCell, r.MakeStore, posixfs.NewDirFS(filepath.Join(r.rootPath, specDirPath)))
	if _, err := branches.CreateIfNotExists(ctx, r.specDir, nameMaster, branches.NewParams(false)); err != nil {
		return nil, err
	}
	r.space, err = branches.NewMultiSpace([]branches.Layer{
		{Prefix: "", Target: r.specDir},
	})
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Repo) Close() (retErr error) {
	for _, fn := range []func() error{
		r.db.Sync,
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
	return r.space
}

func (r *Repo) GetACL() *Policy {
	return r.policy
}

func (r *Repo) getSubFS(prefix string) posixfs.FS {
	return posixfs.NewPrefixed(r.repoFS, prefix)
}

func (r *Repo) getDataOp(b *branches.Branch) gdat.Operator {
	return gdat.NewOperator(
		gdat.WithSalt(b.Salt),
	)
}

func (r *Repo) getFSOp(b *branches.Branch) *gotfs.Operator {
	fsop := gotfs.NewOperator(
		gotfs.WithDataOperator(r.getDataOp(b)),
		gotfs.WithSeed(b.Salt),
	)
	return &fsop
}

func (r *Repo) getVCOp(b *branches.Branch) *gotvc.Operator {
	vcop := gotvc.NewOperator(
		gotvc.WithDataOperator(r.getDataOp(b)),
	)
	return &vcop
}

func (r *Repo) UnionStore() cadata.Store {
	return stores.AssertReadOnly(r.storeManager.store)
}

func dbPath(x string) string {
	return filepath.Join(x, gotPrefix, "local.db")
}

func dumpBucket(w io.Writer, b *bolt.Bucket) {
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		fmt.Fprintf(w, "%q -> %q\n", k, v)
	}
	fmt.Fprintln(w)
}

func (r *Repo) makeDefaultVolume() VolumeSpec {
	newRandom := func() *uint64 {
		x := randomUint64()
		return &x
	}
	cellSpec := CellSpec{
		Local: (*LocalCellSpec)(newRandom()),
	}
	cellSpec = CellSpec{
		Encrypted: &EncryptedCellSpec{
			Inner:  cellSpec,
			Secret: generateSecret(32),
		},
	}
	return VolumeSpec{
		Cell:     cellSpec,
		VCStore:  StoreSpec{Local: (*LocalStoreSpec)(newRandom())},
		FSStore:  StoreSpec{Local: (*LocalStoreSpec)(newRandom())},
		RawStore: StoreSpec{Local: (*LocalStoreSpec)(newRandom())},
	}
}

func generateSecret(n int) []byte {
	x := make([]byte, n)
	if _, err := rand.Read(x); err != nil {
		panic(err)
	}
	return x
}

func randomUint64() uint64 {
	buf := [8]byte{}
	if _, err := rand.Read(buf[:]); err != nil {
		panic(err)
	}
	return binary.BigEndian.Uint64(buf[:])
}
