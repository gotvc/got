package gotrepo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	"blobcache.io/blobcache/src/bchttp"
	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/bcremote"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"github.com/cloudflare/circl/sign/ed25519"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/state"
	"go.brendoncarroll.net/state/kv"
	"go.brendoncarroll.net/state/posixfs"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"

	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/gotorg"
	"github.com/gotvc/got/src/gotrepo/internal/reposchema"
	"github.com/gotvc/got/src/gotvc"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/gotvc/got/src/internal/volumes"
)

// fs paths
const (
	gotPrefix  = ".got"
	configPath = ".got/config"

	localDBPath      = ".got/got.db"
	blobcacheDirPath = ".got/blobcache"
	idenPath         = ".got/iden"
)

type (
	Space  = branches.Space
	Volume = branches.Volume

	Ref  = gotkv.Ref
	Root = gotfs.Root

	Snap = gotvc.Snap
)

// Repo manages configuration including the connection to Blobcache
// The Repo can optionally host it's own Blobcache Node.
// Repos also manage a namespace and multiple stages.
// Working Copies can be created to manipulate the contents of the stages.
type Repo struct {
	rootPath string
	repoFS   posixfs.FS // repoFS is the directory that the repo is in
	config   Config
	bc       blobcache.Service

	idenStore   idenStore
	leafPrivate gotorg.IdenPrivate
	repoc       reposchema.Client
	gnsc        gotorg.Client
	space       lazySetup[branches.Space]
}

// Init initializes a new repo at the given path.
// If bc is nil, a local blobcache will be created within the .got directory.
func Init(p string, config Config) error {
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
	if err := SaveConfig(repoDirFS, configPath, config); err != nil {
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
	ctx, cf := context.WithCancel(ctx)
	defer cf()
	log, _ := zap.NewProduction()
	ctx = logctx.NewContext(ctx, log)

	repoFS := posixfs.NewDirFS(p)
	config, err := LoadConfig(repoFS, configPath)
	if err != nil {
		return nil, err
	}

	// setup identity store
	if err := os.Mkdir(filepath.Join(p, idenPath), 0o755); err != nil && !os.IsExist(err) {
		return nil, err
	}
	idenRoot, err := os.OpenRoot(filepath.Join(p, idenPath))
	if err != nil {
		return nil, err
	}
	idens := idenStore{root: idenRoot}
	idp, err := idens.GetOrCreate("default")
	if err != nil {
		return nil, err
	}
	sigPriv := idp.SigPrivateKey.(ed25519.PrivateKey)

	// blobcache
	var bc blobcache.Service
	switch {
	case config.Blobcache.InProcess != nil:
		if err := posixfs.MkdirAll(repoFS, blobcacheDirPath, 0o755); err != nil {
			return nil, err
		}
		bc, err = openLocalBlobcache(ctx, sigPriv, filepath.Join(p, blobcacheDirPath))
		if err != nil {
			return nil, err
		}
	case config.Blobcache.HTTP != nil:
		bc, err = openHTTPBlobcache(config.Blobcache.HTTP.URL)
		if err != nil {
			return nil, err
		}
	case config.Blobcache.Remote != nil:
		pc, err := net.ListenUDP("udp", nil)
		if err != nil {
			return nil, err
		}
		bc, err = openRemoteBlobcache(sigPriv, pc, config.Blobcache.Remote.Endpoint)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("empty blobcache spec: %v", config.Blobcache)
	}

	r := &Repo{
		rootPath:    p,
		repoFS:      repoFS,
		bc:          bc,
		config:      *config,
		idenStore:   idens,
		leafPrivate: *idp,
		repoc:       reposchema.NewClient(bc),
		gnsc: gotorg.Client{
			Machine:   gotorg.New(),
			Blobcache: bc,
			ActAs:     *idp,
		},
	}
	r.space = newLazySetup(func(ctx context.Context) (branches.Space, error) {
		nsh, err := r.repoc.GetNamespace(ctx, config.RepoVolume, r.useSchema())
		if err != nil {
			return nil, err
		}
		idenUnit, err := r.ActiveIdentity(ctx)
		if err != nil {
			return nil, err
		}
		if err := r.gnsc.EnsureInit(ctx, *nsh, []gotorg.IdentityUnit{*idenUnit}); err != nil {
			return nil, err
		}
		spaceSpec := config.Spaces
		spaceSpec = append(spaceSpec, SpaceLayerSpec{
			Prefix: "",
			Target: SpaceSpec{Local: &nsh.OID},
		})
		space, err := r.MakeSpace(ctx, SpaceSpec{Multi: &spaceSpec})
		if err != nil {
			return nil, err
		}
		// create the master branch if it doesn't exist
		if _, err := branches.CreateIfNotExists(ctx, space, nameMaster, branches.NewConfig(false)); err != nil {
			return nil, err
		}
		return space, nil
	})

	return r, nil
}

func (r *Repo) Close() (retErr error) {
	ctx := context.TODO()
	for _, fn := range []func() error{
		func() error {
			if lsvc, ok := r.bc.(*bclocal.Service); ok {
				logctx.Infof(ctx, "closing in-process blobcache")
				return lsvc.Close()
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

// RootPath is the path given when the Repo was opened
func (r *Repo) RootPath() string {
	return r.rootPath
}

func (r *Repo) GetSpace(ctx context.Context) (Space, error) {
	return r.space.Use(ctx)
}

func (r *Repo) Serve(ctx context.Context, pc net.PacketConn) error {
	svc, ok := r.bc.(*bclocal.Service)
	if !ok {
		return fmt.Errorf("Serve called on repo without in-process Blobcache: %T", r.bc)
	}
	return svc.Serve(ctx, pc)
}

func (r *Repo) Endpoint() blobcache.Endpoint {
	ep, err := r.bc.Endpoint(context.TODO())
	if err != nil {
		panic(err)
	}
	return ep
}

func (r *Repo) Cleanup(ctx context.Context) error {
	// TODO
	return nil
}

// GotNSVolume returns the FQOID of the namespace volume.
// This can be used to access the namespace from another Blobcache node.
// It does not modify the contents of the namespace volume.
func (r *Repo) GotNSVolume(ctx context.Context) (blobcache.FQOID, error) {
	ep, err := r.bc.Endpoint(ctx)
	if err != nil {
		return blobcache.FQOID{}, err
	}
	nsh, err := r.repoc.GetNamespace(ctx, r.config.RepoVolume, r.useSchema())
	if err != nil {
		return blobcache.FQOID{}, err
	}
	return blobcache.FQOID{
		Peer: ep.Peer,
		OID:  nsh.OID,
	}, nil
}

// BeginStagingTx begins a new transaction for the staging area with the given paramHash.
// It is up to the caller to commit or abort the transaction.
func (r *Repo) BeginStagingTx(ctx context.Context, paramHash *[32]byte, mutate bool) (volumes.Tx, error) {
	h, err := r.repoc.StagingArea(ctx, r.config.RepoVolume, paramHash)
	if err != nil {
		return nil, err
	}
	vol := volumes.Blobcache{Service: r.bc, Handle: *h}
	return vol.BeginTx(ctx, blobcache.TxParams{Modify: mutate})
}

func (r *Repo) useSchema() bool {
	bccfg := r.config.Blobcache
	switch {
	case bccfg.HTTP != nil:
		return bccfg.HTTP.UseSchema
	case bccfg.Remote != nil:
		return bccfg.Remote.UseSchema
	default:
		return true
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

func openLocalBlobcache(bgCtx context.Context, privKey ed25519.PrivateKey, p string) (*bclocal.Service, error) {
	// TODO: we should probably let the caller do this.
	logger := zap.NewNop()
	logger.Core().Enabled(zap.PanicLevel)
	bgCtx = logctx.NewContext(bgCtx, logger)

	return bclocal.New(bclocal.Env{
		Background: bgCtx,
		StateDir:   p,
		PrivateKey: privKey,
		MkSchema:   mkSchema,
		Root:       reposchema.GotRepoVolumeSpec(),
		Policy:     &bclocal.AllOrNothingPolicy{},
	}, bclocal.Config{})
}

func openHTTPBlobcache(u string) (*bchttp.Client, error) {
	return bchttp.NewClient(nil, u), nil
}

func openRemoteBlobcache(privateKey ed25519.PrivateKey, pc net.PacketConn, ep blobcache.Endpoint) (*bcremote.Service, error) {
	return bcremote.New(privateKey, pc, ep), nil
}

func mkSchema(spec blobcache.SchemaSpec) (schema.Schema, error) {
	switch spec.Name {
	case reposchema.SchemaName_GotRepo:
		return reposchema.Constructor(spec.Params, nil)
	case reposchema.SchemeName_GotOrg:
		return gotorg.SchemaConstructor(spec.Params, nil)
	case "":
		return schema.None{}, nil
	default:
		return nil, fmt.Errorf("unknown schema %q", spec.Name)
	}
}

// RepoVolumeSpec returns a Blobcache Volume spec which
// can be used to create a Volume suitable for a Repo.
func RepoVolumeSpec(useSchema bool) blobcache.VolumeSpec {
	spec := reposchema.GotRepoVolumeSpec()
	if !useSchema {
		spec.Local.Schema = blobcache.SchemaSpec{
			Name: blobcache.Schema_NONE,
		}
	}
	return spec
}

func NewTestRepo(t testing.TB) *Repo {
	dirpath := t.TempDir()
	t.Log("testing in", dirpath)
	require.NoError(t, Init(dirpath, DefaultConfig()))
	repo, err := Open(dirpath)
	require.NoError(t, err)
	require.NotNil(t, repo)
	t.Cleanup(func() {
		ctx := testutil.Context(t)
		// also run cleanup after every test, to make sure that cleanup works after all the situations we are testing.
		require.NoError(t, repo.Cleanup(ctx))
		require.NoError(t, repo.Close())
	})
	return repo
}
