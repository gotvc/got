package gotrepo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"github.com/cloudflare/circl/sign/ed25519"
	"github.com/cockroachdb/pebble"
	"go.brendoncarroll.net/state"
	"go.brendoncarroll.net/state/kv"
	"go.brendoncarroll.net/state/posixfs"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"

	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/gotns"
	"github.com/gotvc/got/src/gotrepo/internal/dbmig"
	"github.com/gotvc/got/src/gotrepo/internal/reposchema"
	"github.com/gotvc/got/src/gotvc"
	"github.com/gotvc/got/src/internal/dbutil"
	"github.com/gotvc/got/src/internal/migrations"
)

// fs paths
const (
	gotPrefix      = ".got"
	configPath     = ".got/config"
	privateKeyPath = ".got/private.pem"

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

	bcDB   *pebble.DB
	bc     blobcache.Service
	db     *dbutil.Pool
	config Config
	// ctx is used as the background context for serving the repo
	ctx context.Context

	leafPrivate gotns.LeafPrivate
	workingDir  FS // workingDir is repoFS with reserved paths filtered.
	repoc       reposchema.Client
	gnsc        gotns.Client
	space       branches.Space
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
	// config
	config := DefaultConfig()
	if err := SaveConfig(repoDirFS, configPath, config); err != nil {
		return err
	}
	r, err := Open(p)
	if err != nil {
		return err
	}
	ctx := context.TODO()
	idenLeaf, err := r.ActiveIdentity(ctx)
	if err != nil {
		return err
	}

	nsh, err := r.repoc.Namespace(ctx)
	if err != nil {
		return err
	}
	if err := r.gnsc.Init(ctx, *nsh, []gotns.IdentityLeaf{idenLeaf}); err != nil {
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
	db, err := dbutil.OpenPool(filepath.Join(p, localDBPath))
	if err != nil {
		return nil, err
	}
	var leafPrivate *gotns.LeafPrivate
	if err := dbutil.Borrow(ctx, db, func(conn *dbutil.Conn) error {
		if err := migrations.EnsureAll(conn, dbmig.ListMigrations()); err != nil {
			return err
		}
		if err := setupIdentity(conn); err != nil {
			return err
		}
		leafPrivate, err = loadIdentity(conn)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// blobcache
	if err := posixfs.MkdirAll(repoFS, blobcacheDirPath, 0o755); err != nil {
		return nil, err
	}
	bc, bcDB, err := openLocalBlobcache(filepath.Join(p, blobcacheDirPath))
	if err != nil {
		return nil, err
	}

	r := &Repo{
		rootPath:    p,
		repoFS:      repoFS,
		db:          db,
		bc:          bc,
		bcDB:        bcDB,
		config:      *config,
		leafPrivate: *leafPrivate,
		ctx:         ctx,
		repoc:       reposchema.NewClient(bc),
		gnsc: gotns.Client{
			Machine:   gotns.New(),
			Blobcache: bc,
			ActAs:     *leafPrivate,
		},
		workingDir: posixfs.NewFiltered(repoFS, func(x string) bool {
			return !strings.HasPrefix(x, gotPrefix)
		}),
	}

	nsh, err := r.repoc.Namespace(ctx)
	if err != nil {
		return nil, err
	}

	// setup space
	spaceSpec := config.Spaces
	spaceSpec = append(spaceSpec, SpaceLayerSpec{
		Prefix: "",
		Target: SpaceSpec{Local: &nsh.OID},
	})
	space, err := r.MakeSpace(SpaceSpec{Multi: &spaceSpec})
	if err != nil {
		return nil, err
	}
	r.space = space
	return r, nil
}

func (r *Repo) Close() (retErr error) {
	ctx := context.TODO()
	for _, fn := range []func() error{
		func() error {
			return dbutil.Borrow(context.TODO(), r.db, func(conn *dbutil.Conn) error {
				return dbutil.WALCheckpoint(conn)
			})
		},
		r.db.Close,
		func() error {
			// close the blobcache database, if it exists
			// r.bcDB is only set if the in-process blobcache is used.
			if r.bcDB != nil {
				if err := r.bc.(*bclocal.Service).AbortAll(ctx); err != nil {
					logctx.Warn(ctx, "error aborting blobcache transactions", zap.Error(err))
				}
				logctx.Infof(ctx, "closing pebble db")
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

func (r *Repo) Serve(ctx context.Context, pc net.PacketConn) error {
	edPriv, ok := r.leafPrivate.SigPrivateKey.(ed25519.PrivateKey)
	if !ok {
		return fmt.Errorf("repo.Serve: signing key must be ed25519")
	}
	blobDirPath := filepath.Join(r.rootPath, blobcacheDirPath, "blob")
	blobDir, err := os.OpenRoot(blobDirPath)
	if err != nil {
		return err
	}
	svc := bclocal.New(bclocal.Env{
		DB:      r.bcDB,
		BlobDir: blobDir,

		PrivateKey: edPriv,
		PacketConn: pc,

		Schemas: blobcacheSchemas(),
		Root:    reposchema.GotRepoVolumeSpec(),
	}, bclocal.Config{})
	r.bc = svc
	return svc.Run(ctx)
}

// Cleanup removes unreferenced data from the repo's local DB.
func (r *Repo) Cleanup(ctx context.Context) error {
	if err := dbutil.DoTx(ctx, r.db, func(conn *dbutil.Conn) error {
		logctx.Infof(ctx, "removing blobs from staging areas")
		if err := r.cleanupStagingBlobs(ctx, conn); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if err := dbutil.Borrow(ctx, r.db, func(conn *dbutil.Conn) error {
		logctx.Infof(ctx, "truncating WAL...")
		if err := dbutil.WALCheckpoint(conn); err != nil {
			return err
		}
		logctx.Infof(ctx, "running VACUUM...")
		if err := dbutil.Vacuum(conn); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (r *Repo) Endpoint() blobcache.Endpoint {
	ep, err := r.bc.Endpoint(context.TODO())
	if err != nil {
		panic(err)
	}
	return ep
}

// GotNSVolume returns the FQOID of the namespace volume.
// This can be used to access the namespace from another Blobcache node.
func (r *Repo) GotNSVolume(ctx context.Context) (blobcache.FQOID, error) {
	ep, err := r.bc.Endpoint(r.ctx)
	if err != nil {
		return blobcache.FQOID{}, err
	}
	nsh, err := r.repoc.Namespace(r.ctx)
	if err != nil {
		return blobcache.FQOID{}, err
	}
	return blobcache.FQOID{
		Peer: ep.Peer,
		OID:  nsh.OID,
	}, nil
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

func openLocalBlobcache(p string) (*bclocal.Service, *pebble.DB, error) {
	pebbleDirPath := filepath.Join(p, "pebble")
	blobDirPath := filepath.Join(p, "blob")
	for _, dir := range []string{pebbleDirPath, blobDirPath} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, nil, err
		}
	}
	db, err := pebble.Open(pebbleDirPath, &pebble.Options{
		Logger: zap.NewNop().Sugar(),
	})
	if err != nil {
		return nil, nil, err
	}
	blobDir, err := os.OpenRoot(blobDirPath)
	if err != nil {
		return nil, nil, err
	}

	return bclocal.New(bclocal.Env{
		DB:      db,
		BlobDir: blobDir,
		Schemas: blobcacheSchemas(),
		Root:    reposchema.GotRepoVolumeSpec(),
	}, bclocal.Config{}), db, nil
}

func blobcacheSchemas() map[blobcache.Schema]schema.Schema {
	schemas := bclocal.DefaultSchemas()
	schemas[reposchema.SchemaName_GotRepo] = reposchema.NewSchema()
	schemas[reposchema.SchemaName_GotNS] = gotns.Schema{}
	return schemas
}
