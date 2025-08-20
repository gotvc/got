package gotrepo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/state"
	"go.brendoncarroll.net/state/kv"
	"go.brendoncarroll.net/state/posixfs"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.inet256.org/inet256/src/inet256"
	"go.uber.org/zap"

	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/gotns"
	"github.com/gotvc/got/src/gotrepo/internal/dbmig"
	"github.com/gotvc/got/src/gotvc"
	"github.com/gotvc/got/src/internal/dbutil"
	"github.com/gotvc/got/src/internal/migrations"
	"github.com/jmoiron/sqlx"
)

// fs paths
const (
	gotPrefix      = ".got"
	configPath     = ".got/config"
	privateKeyPath = ".got/private.pem"
	specDirPath    = ".got/branches"

	localDBPath      = ".got/got.db"
	blobcacheDirPath = ".got/blobcache"
)

type (
	FS = posixfs.FS

	Space  = branches.Space
	Volume = branches.Volume

	Ref  = gotkv.Ref
	Root = gotfs.Root

	Snap = gotvc.Snap
)

type Repo struct {
	rootPath string
	repoFS   FS // repoFS is the directory that the repo is in

	bcDB       *sqlx.DB
	bc         blobcache.Service
	db         *sqlx.DB
	config     Config
	privateKey inet256.PrivateKey
	// ctx is used as the background context for serving the repo
	ctx context.Context

	workingDir FS // workingDir is repoFS with reserved paths filtered.
	gnsc       *gotns.Client
	space      branches.Space
}

// Init initializes a new repo at the given path.
// If bc is nil, a local blobcache will be created within the .got directory.
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
	r, err := Open(p)
	if err != nil {
		return err
	}
	ctx := context.TODO()
	if err := r.gnsc.Init(ctx, blobcache.Handle{}); err != nil {
		return err
	}
	if _, err := branches.CreateIfNotExists(ctx, r.space, nameMaster, branches.NewConfig(false)); err != nil {
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
	db, err := dbutil.OpenDB(filepath.Join(p, localDBPath))
	if err != nil {
		return nil, err
	}
	if err := dbutil.DoTx(ctx, db, func(tx *sqlx.Tx) error {
		return migrations.EnsureAll(tx, dbmig.ListMigrations())
	}); err != nil {
		return nil, err
	}

	privateKey, err := LoadPrivateKey(repoFS, privateKeyPath)
	if err != nil {
		return nil, err
	}

	// blobcache
	var bc blobcache.Service
	var bcDB *sqlx.DB
	switch {
	case config.Blobcache.InProcess != nil:
		if err := posixfs.MkdirAll(repoFS, blobcacheDirPath, 0o755); err != nil {
			return nil, err
		}
		bc, bcDB, err = openLocalBlobcache(ctx, filepath.Join(p, blobcacheDirPath))
		if err != nil {
			return nil, err
		}
	case config.Blobcache.HTTP != nil:
		bc = bcclient.NewClient(*config.Blobcache.HTTP)
	default:
		return nil, fmt.Errorf("must configure blobcache in .got/config.  It cannot be empty.")
	}

	r := &Repo{
		rootPath:   p,
		repoFS:     repoFS,
		db:         db,
		bc:         bc,
		bcDB:       bcDB,
		config:     *config,
		privateKey: privateKey,
		ctx:        ctx,

		workingDir: posixfs.NewFiltered(repoFS, func(x string) bool {
			return !strings.HasPrefix(x, gotPrefix)
		}),
	}
	r.gnsc = &gotns.Client{
		Blobcache: r.bc,
		Machine:   gotns.New(),
	}
	r.space = gotns.NewSpace(r.gnsc, blobcache.Handle{})
	return r, nil
}

func (r *Repo) Close() (retErr error) {
	for _, fn := range []func() error{
		r.db.Close,
		func() error {
			if r.bcDB != nil {
				return r.bcDB.Close()
			}
			return nil
		},
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

func (r *Repo) Serve(ctx context.Context) error {
	return r.bc.(*bclocal.Service).Run(ctx)
}

func (r *Repo) GetEndpoint(ctx context.Context) blobcache.Endpoint {
	ep, err := r.bc.Endpoint(ctx)
	if err != nil {
		panic(err)
	}
	return ep
}

func (r *Repo) GetFQOID() blobcache.FQOID {
	ep, err := r.bc.Endpoint(r.ctx)
	if err != nil {
		panic(err)
	}
	return blobcache.FQOID{
		Peer: ep.Peer,
	}
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

func (r *Repo) defaultVolumeSpec(_ context.Context) (VolumeSpec, error) {
	spec := blobcache.DefaultLocalSpec()
	spec.Local.HashAlgo = blobcache.HashAlgo_BLAKE2b_256
	spec.Local.MaxSize = 1 << 21
	return spec, nil
}

func openLocalBlobcache(ctx context.Context, p string) (blobcache.Service, *sqlx.DB, error) {
	db, err := dbutil.OpenDB(filepath.Join(p, "blobcache.db"))
	if err != nil {
		return nil, nil, err
	}
	if err := bclocal.SetupDB(ctx, db); err != nil {
		return nil, nil, err
	}
	const Schema_GOTNS = blobcache.Schema("gotns")
	schemas := bclocal.DefaultSchemas()
	schemas[Schema_GOTNS] = gotns.Schema{}
	rootSpec := blobcache.DefaultLocalSpec()
	rootSpec.Local.Schema = Schema_GOTNS
	return bclocal.New(bclocal.Env{
		DB:      db,
		Schemas: schemas,
		Root:    rootSpec,
	}), db, nil
}
