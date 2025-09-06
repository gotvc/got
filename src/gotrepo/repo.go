package gotrepo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"github.com/cloudflare/circl/sign"
	"github.com/cloudflare/circl/sign/ed25519"
	"github.com/jmoiron/sqlx"
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

	bcDB   *sqlx.DB
	bc     blobcache.Service
	db     *dbutil.Pool
	config Config
	// ctx is used as the background context for serving the repo
	ctx context.Context

	privateKey sign.PrivateKey
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
	if err := r.gnsc.Init(ctx, blobcache.Handle{}, []gotns.IdentityLeaf{idenLeaf}); err != nil {
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
	var privateKey sign.PrivateKey
	if err := dbutil.Borrow(ctx, db, func(conn *dbutil.Conn) error {
		if err := migrations.EnsureAll(conn, dbmig.ListMigrations()); err != nil {
			return err
		}
		if err := setupIdentity(conn); err != nil {
			return err
		}
		privateKey, _, err = loadIdentity(conn)
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
	bc, bcDB, err := openLocalBlobcache(ctx, filepath.Join(p, blobcacheDirPath))
	if err != nil {
		return nil, err
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

	// setup space
	gnsc := r.GotNSClient()
	r.gnsc = &gnsc
	spaceSpec := config.Spaces
	spaceSpec = append(spaceSpec, SpaceLayerSpec{
		Prefix: "",
		Target: SpaceSpec{Local: &blobcache.OID{}},
	})
	space, err := r.MakeSpace(SpaceSpec{Multi: &spaceSpec})
	if err != nil {
		return nil, err
	}
	r.space = space
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

func (r *Repo) Serve(ctx context.Context, pc net.PacketConn) error {
	svc := bclocal.New(bclocal.Env{
		DB:         r.bcDB,
		PrivateKey: r.privateKey.(ed25519.PrivateKey),
		PacketConn: pc,
		Schemas:    blobcacheSchemas(),
		Root:       *blobcacheRootSpec(),
	})
	r.bc = svc
	return svc.Run(ctx)
}

// Cleanup removes unreferenced data from the repo's local DB.
func (r *Repo) Cleanup(ctx context.Context) error {
	if err := dbutil.DoTx(ctx, r.db, func(conn *dbutil.Conn) error {
		logctx.Infof(ctx, "removing blobs from staging areas")
		if err := cleanupStagingBlobs(ctx, conn); err != nil {
			return err
		}
		logctx.Infof(ctx, "removing unreferenced blobs")
		if err := cleanupBlobs(conn); err != nil {
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

func openLocalBlobcache(ctx context.Context, p string) (*bclocal.Service, *sqlx.DB, error) {
	db, err := dbutil.OpenSQLxDB(filepath.Join(p, "blobcache.db"))
	if err != nil {
		return nil, nil, err
	}
	// TODO: remove this.
	// This prevents a sqlite BUSY error.
	db.SetMaxOpenConns(1)
	if err := bclocal.SetupDB(ctx, db); err != nil {
		return nil, nil, err
	}
	return bclocal.New(bclocal.Env{
		DB:      db,
		Schemas: blobcacheSchemas(),
		Root:    *blobcacheRootSpec(),
	}), db, nil
}

const schema_GOTNS = blobcache.Schema("gotns")

func blobcacheSchemas() map[blobcache.Schema]schema.Schema {
	schemas := bclocal.DefaultSchemas()
	schemas[schema_GOTNS] = gotns.Schema{}
	rootSpec := blobcache.DefaultLocalSpec()
	rootSpec.Local.Schema = schema_GOTNS
	rootSpec.Local.MaxSize = 1 << 22
	return schemas
}

func blobcacheRootSpec() *blobcache.VolumeSpec {
	rootSpec := blobcache.DefaultLocalSpec()
	rootSpec.Local.Schema = schema_GOTNS
	rootSpec.Local.MaxSize = 1 << 22
	return &rootSpec
}
