package gotrepo

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/state"
	"go.brendoncarroll.net/state/kv"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/gotorg"
	"github.com/gotvc/got/src/gotrepo/internal/reposchema"
	"github.com/gotvc/got/src/internal/gotbc"
	"github.com/gotvc/got/src/internal/gotcfg"
	"github.com/gotvc/got/src/internal/gotcore"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/gotvc/got/src/internal/volumes"
)

// fs paths
const (
	gotPrefix  = ".got"
	configPath = ".got/config"

	idenPath = ".got/iden"
)

type (
	Space  = gotcore.Space
	Commit = gotcore.Commit
	Ref    = gotkv.Ref
	Root   = gotfs.Root
)

// Repos manage a namespace and multiple stages.
// Working Copies can be created to manipulate the contents of the stages.
type Repo struct {
	bc blobcache.Service
	// rootVol is the OID of the Volume in Blobcache holding the Repo
	rootVol blobcache.OID

	// dir is the path to the directory containing the repo config.
	// this is being phased out.
	dir *os.Root

	config Config
	repoc  reposchema.Client
}

// Init initializes a new repo at the provided Volume in Blobcache.
func Init(ctx context.Context, bc blobcache.Service, volh blobcache.Handle, config Config) error {
	rc := reposchema.NewClient(bc)
	cfgData, err := rc.GetConfig(ctx, volh.OID)
	if err != nil {
		return err
	}
	if len(cfgData) != 0 {
		return fmt.Errorf("repo volume has already been initialized")
	}
	if err := rc.EditConfig(ctx, volh.OID, func(x json.RawMessage) json.RawMessage {
		return gotcfg.Marshal(config)
	}); err != nil {
		return err
	}
	return nil
}

func Open(ctx context.Context, bc blobcache.Service, volid blobcache.OID, dir *os.Root) (_ *Repo, retErr error) {
	if dir != nil {
		// if there is a directory then config must be present
		_, err := LoadConfig(dir)
		if err != nil {
			return nil, err
		}
		if err := dir.MkdirAll(idenPath, 0o755); err != nil && !os.IsExist(err) {
			return nil, err
		}
	}
	r := New(bc, volid, dir)
	idens, err := r.Identities(ctx)
	if err != nil {
		return nil, err
	}
	if len(idens) == 0 {
		_, err := r.CreateIdentity(ctx, DefaultIden)
		if err != nil {
			return nil, err
		}
	}
	if err := r.reloadConfig(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

// New creates a new repo once the environment is setup.
func New(bc blobcache.Service, vol blobcache.OID, dir *os.Root) *Repo {
	return &Repo{
		bc:      bc,
		rootVol: vol,

		dir: dir, // TODO: remove

		repoc: reposchema.NewClient(bc),
	}
}

func (r *Repo) Blobcache() blobcache.Service {
	return r.bc
}

func (r *Repo) Config() Config {
	return r.config
}

func (r *Repo) Cleanup(ctx context.Context) error {
	// TODO
	return nil
}

func (r *Repo) OrgClient(actAs string) (gotorg.Client, error) {
	idp, err := r.getPrivate(context.TODO(), actAs)
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
	nsh, _, err := r.repoc.GetNamespace(ctx, r.rootVol, false)
	if err != nil {
		return blobcache.FQOID{}, err
	}
	return blobcache.FQOID{
		Node: ep.Node,
		OID:  nsh.OID,
	}, nil
}

func (r *Repo) NSVolumeSpec(ctx context.Context) (*VolumeSpec, error) {
	ep, err := r.bc.Endpoint(ctx)
	if err != nil {
		return nil, err
	}
	nsh, secret, err := r.repoc.GetNamespace(ctx, r.rootVol, false)
	if err != nil {
		return nil, err
	}
	return &VolumeSpec{
		URL: blobcache.URL{
			Node:   ep.Node,
			IPPort: &ep.IPPort,
			OID:    nsh.OID,
		},
		Secret: *secret,
	}, nil
}

// BeginStagingTx begins a new transaction for the staging area for the given WorkingCopy
// It is up to the caller to commit or abort the transaction.
func (r *Repo) BeginStagingTx(ctx context.Context, wcid WorkingCopyID, modify bool) (volumes.Tx, error) {
	if wcid == (WorkingCopyID{}) {
		return nil, fmt.Errorf("working copy id cannot be 0")
	}
	h, dek, err := r.repoc.StagingArea(ctx, r.rootVol, wcid)
	if err != nil {
		return nil, err
	}
	var vol volumes.Volume = &volumes.Blobcache{Service: r.bc, Handle: *h}
	vol = volumes.NewChaCha20Poly1305(vol, (*[32]byte)(dek))
	return vol.BeginTx(ctx, blobcache.TxParams{Modify: modify})
}

// GCStage begins a new GC transaction for the staging area.
func (r *Repo) GCStage(ctx context.Context, wcid WorkingCopyID) (volumes.Tx, error) {
	h, dek, err := r.repoc.StagingArea(ctx, r.rootVol, wcid)
	if err != nil {
		return nil, err
	}
	var vol volumes.Volume = &volumes.Blobcache{Service: r.bc, Handle: *h}
	vol = volumes.NewChaCha20Poly1305(vol, (*[32]byte)(dek))
	return vol.BeginTx(ctx, blobcache.TxParams{
		Modify:  true,
		GCBlobs: true,
	})
}

type WorkingCopyID = reposchema.StageID

func NewWorkingCopyID() WorkingCopyID {
	return reposchema.NewStageID()
}

// reloadConfig loads the config back in to the repo.
func (r *Repo) reloadConfig(ctx context.Context) error {
	// if we still have a legacy dir, then that config is authoritive.
	if r.dir != nil {
		cfg, err := LoadConfig(r.dir)
		if err != nil {
			return err
		}
		if err := r.repoc.EditConfig(ctx, r.rootVol, func(json.RawMessage) json.RawMessage {
			return gotcfg.Marshal(cfg) // blind overwrite
		}); err != nil {
			return err
		}
	}
	cfgData, err := r.repoc.GetConfig(ctx, r.rootVol)
	if err != nil {
		return err
	}
	cfg, err := gotcfg.Parse[Config](cfgData)
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

// NewTestRepo creates an new Volume in blobcache and initializes a new repo in it.
func NewTestRepo(t testing.TB, bc blobcache.Service) *Repo {
	ctx := t.Context()
	volh, err := bc.CreateVolume(ctx, nil, gotbc.GotVolumeSpec())
	require.NoError(t, err)
	require.NoError(t, Init(ctx, bc, *volh, DefaultConfig()))
	repo, err := Open(ctx, bc, volh.OID, nil)
	require.NoError(t, err)
	require.NotNil(t, repo)
	t.Cleanup(func() {
		ctx := testutil.Context(t)
		// also run cleanup after every test, to make sure that cleanup works after all the situations we are testing.
		require.NoError(t, repo.Cleanup(ctx))
	})
	return repo
}

// RepoVolumeSpec returns a Blobcache Volume spec which
// can be used to create a Volume suitable for a Repo.
func RepoVolumeSpec(useSchema bool) blobcache.VolumeSpec {
	spec := gotbc.GotVolumeSpec()
	if useSchema {
		spec.Local.Schema = blobcache.SchemaSpec{
			Name: reposchema.SchemaName_GotRepo,
		}
	}
	return spec
}
