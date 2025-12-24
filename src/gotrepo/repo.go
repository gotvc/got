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

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/state"
	"go.brendoncarroll.net/state/kv"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/gotorg"
	"github.com/gotvc/got/src/gotrepo/internal/reposchema"
	"github.com/gotvc/got/src/internal/gotcfg"
	"github.com/gotvc/got/src/internal/marks"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/gotvc/got/src/internal/volumes"
)

// fs paths
const (
	gotPrefix  = ".got"
	configPath = ".got/config"

	blobcacheDirPath = ".got/blobcache"
	idenPath         = ".got/iden"
)

type (
	Space  = marks.Space
	Volume = marks.Volume

	Ref  = gotkv.Ref
	Root = gotfs.Root

	Snap = marks.Snap
)

// Repo manages configuration including the connection to Blobcache
// The Repo can optionally host it's own Blobcache Node.
// Repos also manage a namespace and multiple stages.
// Working Copies can be created to manipulate the contents of the stages.
type Repo struct {
	root     *os.Root
	bgCtx    context.Context
	cf       context.CancelFunc
	config   Config
	bc       blobcache.Service
	closeAll bool

	repoc reposchema.Client
}

// Init initializes a new repo at the given path.
// If bc is nil, a local blobcache will be created within the .got directory.
func Init(p string, config Config) error {
	root, err := os.OpenRoot(p)
	if err != nil {
		return err
	}
	defer root.Close()
	if err := root.Mkdir(gotPrefix, 0o755); err != nil {
		return err
	}
	// config
	if err := gotcfg.CreateFile(root, configPath, config); err != nil {
		return fmt.Errorf("could not create repo config, it may already exist. %w", err)
	}
	// check that is opens without error
	r, err := Open(p)
	if err != nil {
		return err
	}
	return r.Close()
}

func Open(p string) (_ *Repo, retErr error) {
	p, err := filepath.Abs(p)
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(p)
	if err != nil {
		return nil, err
	}

	// config
	config, err := LoadConfig(root)
	if err != nil {
		return nil, err
	}
	// setup identity store
	if err := root.MkdirAll(idenPath, 0o755); err != nil && !os.IsExist(err) {
		return nil, err
	}
	if len(config.Identities) < 1 {
		idens, err := openIdenStore(root)
		if err != nil {
			return nil, err
		}
		defer idens.Close()
		idp := gotorg.GenerateIden()
		id, err := idens.Add(idp)
		if err != nil {
			return nil, err
		}
		if err := EditConfig(root, func(x Config) Config {
			x.Identities[DefaultIden] = id
			return x
		}); err != nil {
			return nil, err
		}
		config, err = LoadConfig(root)
		if err != nil {
			return nil, err
		}
	}
	// background context
	bgCtx := context.Background()
	bgCtx, cf := context.WithCancel(bgCtx)
	defer func() {
		if retErr != nil {
			cf()
		}
	}()
	log, _ := zap.NewProduction()
	bgCtx = logctx.NewContext(bgCtx, log)
	// blobcache
	bc, err := makeBlobcache(root, *config, config.Blobcache, bgCtx)
	if err != nil {
		return nil, err
	}

	env := Env{
		Background: bgCtx,
		Dir:        root,
		Blobcache:  bc,
		CloseAll:   true,
	}
	r := New(env, *config)
	r.cf = cf
	return r, nil
}

// Env is the environment that the Repo needs to function.
type Env struct {
	Background context.Context
	Dir        *os.Root
	Blobcache  blobcache.Service

	// CloseAll causes Repo.Close to also close these resources.
	CloseAll bool
}

// New creates a new repo once the environment is setup.
func New(env Env, cfg Config) *Repo {
	return &Repo{
		root:     env.Dir,
		bc:       env.Blobcache,
		config:   cfg,
		closeAll: env.CloseAll,

		repoc: reposchema.NewClient(env.Blobcache),
	}
}

func (r *Repo) Close() (retErr error) {
	ctx := context.TODO()
	for _, fn := range []func() error{
		func() error {
			if r.cf != nil {
				r.cf()
			}
			return nil
		},
		func() error {
			if !r.closeAll {
				return nil
			}
			if lsvc, ok := r.bc.(*bclocal.Service); ok {
				logctx.Infof(ctx, "closing in-process blobcache")
				return lsvc.Close()
			}
			return nil
		},
		func() error {
			if !r.closeAll {
				return nil
			}
			return r.root.Close()
		},
	} {
		if err := fn(); err != nil {
			retErr = errors.Join(retErr, err)
		}
	}
	return retErr
}

func (r *Repo) Config() Config {
	return r.config
}

// Dir is the path given when the Repo was opened
func (r *Repo) Dir() string {
	return r.root.Name()
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

func (r *Repo) OrgClient(actAs string) (gotorg.Client, error) {
	idp, err := r.getPrivate(actAs)
	if err != nil {
		return gotorg.Client{}, err
	}
	return gotorg.Client{
		Blobcache: r.bc,
		Machine:   gotorg.New(),
		ActAs:     *idp,
	}, nil
}

// NSVolume returns the FQOID of the namespace volume.
// This can be used to access the namespace from another Blobcache node.
// It does not modify the contents of the namespace volume.
func (r *Repo) NSVolume(ctx context.Context) (blobcache.FQOID, error) {
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

func (r *Repo) NSVolumeURL(ctx context.Context) (*blobcache.URL, error) {
	ep, err := r.bc.Endpoint(ctx)
	if err != nil {
		return nil, err
	}
	nsfqoid, err := r.NSVolume(ctx)
	if err != nil {
		return nil, err
	}
	return &blobcache.URL{
		Node:   nsfqoid.Peer,
		IPPort: &ep.IPPort,
		Base:   nsfqoid.OID,
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
	default:
		return true
	}
}

func (r *Repo) reloadConfig() error {
	cfg, err := LoadConfig(r.root)
	if err != nil {
		return err
	}
	r.config = *cfg
	return nil
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
