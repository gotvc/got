package gotrepo

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"go.brendoncarroll.net/state"
	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/state/kv"
	"go.brendoncarroll.net/state/posixfs"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.inet256.org/inet256/pkg/inet256"
	"go.uber.org/zap"

	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/cells"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gothost"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/gotvc/got/pkg/gotnet"
	"github.com/gotvc/got/pkg/gotrepo/repodb"
	"github.com/gotvc/got/pkg/gotvc"
	"github.com/gotvc/got/pkg/staging"
	"github.com/gotvc/got/pkg/stores"
)

const (
	MaxBlobSize = gotfs.DefaultMaxBlobSize
	MaxCellSize = 1 << 16
)

const (
	tableDefault      = repodb.TableID(1)
	tableStaging      = repodb.TableID(2)
	tablePorter       = repodb.TableID(3)
	tableImportStores = repodb.TableID(4)
	tableImportCaches = repodb.TableID(5)
)

const (
	keyActive  = "ACTIVE"
	nameMaster = "master"
)

// fs paths
const (
	gotPrefix      = ".got"
	configPath     = ".got/config"
	privateKeyPath = ".got/private.pem"
	specDirPath    = ".got/branches"

	localDBPath = ".got/local.db"

	blobsPath   = ".got/blobs"
	storeDBPath = ".got/stores.db"

	cellDBPath = ".got/cells.db"
)

type (
	FS = posixfs.FS

	Cell   = cells.Cell
	Space  = branches.Space
	Volume = branches.Volume
	Store  = cadata.Store

	Ref  = gotkv.Ref
	Root = gotfs.Root

	Snap = gotvc.Snap

	PeerID = gothost.PeerID
)

type Repo struct {
	rootPath string
	repoFS   FS // repoFS is the directory that the repo is in

	localDB  *badger.DB
	storesDB *badger.DB
	cellsDB  *badger.DB

	config     Config
	privateKey inet256.PrivateKey
	// ctx is used as the background context for serving the repo
	ctx context.Context

	workingDir FS // workingDir is repoFS with reserved paths filtered.
	stage      *staging.Stage

	specDir    *branchSpecDir
	space      branches.Space
	hostEngine *gothost.HostEngine

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
		return fmt.Errorf("repo already exists at path %s", p)
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
	log, _ := zap.NewProduction()
	ctx = logctx.NewContext(ctx, log)

	repoFS := posixfs.NewDirFS(p)
	config, err := LoadConfig(repoFS, configPath)
	if err != nil {
		return nil, err
	}
	localDB, err := badger.Open(func() badger.Options {
		opts := badger.DefaultOptions(filepath.Join(p, localDBPath))
		opts = opts.WithCompression(options.None)
		opts = opts.WithCompactL0OnClose(false)
		opts.Logger = nil
		return opts
	}())
	if err != nil {
		return nil, err
	}
	storesDB, err := badger.Open(func() badger.Options {
		opts := badger.DefaultOptions(filepath.Join(p, storeDBPath))
		opts = opts.WithCompression(options.None)
		opts = opts.WithCompactL0OnClose(false)
		opts.Logger = nil
		return opts
	}())
	if err != nil {
		return nil, err
	}
	cellsDB, err := badger.Open(func() badger.Options {
		opts := badger.DefaultOptions(filepath.Join(p, cellDBPath))
		opts = opts.WithCompression(options.None)
		opts = opts.WithCompactL0OnClose(false)
		opts.Logger = nil

		opts.SyncWrites = true
		return opts
	}())
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
		localDB:    localDB,
		storesDB:   storesDB,
		cellsDB:    cellsDB,
		ctx:        ctx,

		workingDir: posixfs.NewFiltered(repoFS, func(x string) bool {
			return !strings.HasPrefix(x, gotPrefix)
		}),
		storeManager: newStoreManager(fsStore, storesDB),
		cellManager:  newCellManager(cellsDB),
		stage:        staging.New(repodb.NewKVStore(localDB, tableStaging)),
	}
	r.specDir = newBranchSpecDir(r.makeDefaultVolume, r.MakeCell, r.MakeStore, posixfs.NewDirFS(filepath.Join(r.rootPath, specDirPath)))
	if r.space, err = r.spaceFromSpecs(r.config.Spaces); err != nil {
		return nil, err
	}
	if _, err := branches.CreateIfNotExists(ctx, r.specDir, nameMaster, branches.NewConfig(false)); err != nil {
		return nil, err
	}
	r.hostEngine = gothost.NewHostEngine(r.specDir)
	if err := r.hostEngine.Initialize(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Repo) Close() (retErr error) {
	for _, fn := range []func() error{
		r.localDB.Sync,
		r.localDB.Close,
		r.cellsDB.Sync,
		r.cellsDB.Close,
		r.storesDB.Sync,
		r.storesDB.Close,
	} {
		if err := fn(); err != nil {
			retErr = errors.Join(retErr, err)
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

func (r *Repo) GetHostEngine() *gothost.HostEngine {
	return r.hostEngine
}

func (r *Repo) getFSOp(b *branches.Info) *gotfs.Machine {
	return branches.NewGotFS(b)
}

func (r *Repo) getVCOp(b *branches.Info) *gotvc.Machine {
	return branches.NewGotVC(b)
}

func (r *Repo) getKVStore(tid repodb.TableID) kv.Store[[]byte, []byte] {
	return repodb.NewKVStore(r.localDB, tid)
}

func (r *Repo) UnionStore() cadata.Store {
	return stores.AssertReadOnly(r.storeManager.store)
}

func dumpStore(ctx context.Context, w io.Writer, s kv.Store[[]byte, []byte]) error {
	if err := kv.ForEach[[]byte](ctx, s, state.TotalSpan[[]byte](), func(k []byte) error {
		v, _ := kv.Get(ctx, s, k)
		_, err := fmt.Fprintf(w, "%q -> %q\n", k, v)
		return err
	}); err != nil {
		return err
	}
	_, err := fmt.Fprintln(w)
	return err
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
