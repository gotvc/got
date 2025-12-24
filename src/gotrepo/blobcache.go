package gotrepo

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"blobcache.io/blobcache/src/bchttp"
	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/bcremote"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"github.com/cloudflare/circl/sign/ed25519"
	"github.com/gotvc/got/src/gotorg"
	"github.com/gotvc/got/src/gotrepo/internal/reposchema"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.inet256.org/inet256/src/inet256"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

// BlobcacheSpec describes how to access a Blobcache Service.
type BlobcacheSpec struct {
	// InProcess uses an in-process Blobcache service.
	// The state will be stored in the .got/blobcache directory.
	// This is the default.
	// The state can get quite large for large datasets, so it is recommended to use the system's Blobcache.
	InProcess *InProcessBlobcache `json:"in_process,omitempty"`
	// HTTP uses an HTTP Blobcache service.
	// This is plaintext, non-encrypted HTTP, and it does not require authentication.
	// This should only be used for connecting on local host or via a unix socket.
	HTTP *HTTPBlobcache `json:"http,omitempty"`
}

type InProcessBlobcache struct {
	ActAs    string       `json:"act_as"`
	CanLook  []inet256.ID `json:"can_look"`
	CanTouch []inet256.ID `json:"can_touch"`
}

type HTTPBlobcache struct {
	URL       string `json:"url"`
	UseSchema bool   `json:"schema"`
}

func makeBlobcache(repo *os.Root, config Config, spec BlobcacheSpec, bgCtx context.Context) (blobcache.Service, error) {
	switch {
	case spec.InProcess != nil:
		if err := repo.MkdirAll(blobcacheDirPath, 0o755); err != nil {
			return nil, err
		}
		localID, ok := config.Identities[spec.InProcess.ActAs]
		if !ok {
			return nil, fmt.Errorf("cannot find identity %q to use for Blobcache", spec.InProcess.ActAs)
		}
		idens, err := openIdenStore(repo)
		if err != nil {
			return nil, err
		}
		defer idens.Close()
		idp, err := idens.Get(localID)
		if err != nil {
			return nil, err
		}
		sigPriv, ok := idp.SigPrivateKey.(ed25519.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("blobcache requires an identity with an Ed25519 signing key")
		}
		pol := newBCPolicy(repo, bgCtx)
		bcPath := filepath.Join(repo.Name(), blobcacheDirPath)
		bc, err := openLocalBlobcache(bgCtx, sigPriv, bcPath, pol)
		if err != nil {
			return nil, err
		}
		return bc, nil
	case spec.HTTP != nil:
		bc, err := openHTTPBlobcache(spec.HTTP.URL)
		if err != nil {
			return nil, err
		}
		return bc, nil
	default:
		return nil, fmt.Errorf("empty blobcache spec: %v", spec)
	}
}

// BlobcachePeer returns the PeerID used by the local Blobcache.
// This depends on the ActAs parameter in the Blobcache config.
// If the Blobcache is not local, then the zero value is returned
func (r *Repo) BlobcachePeer() blobcache.PeerID {
	bcfg := r.config.Blobcache
	if bcfg.InProcess == nil {
		return blobcache.PeerID{}
	}
	return r.config.Identities[bcfg.InProcess.ActAs]
}

func openLocalBlobcache(bgCtx context.Context, privKey ed25519.PrivateKey, stateDir string, pol *bcPolicy) (*bclocal.Service, error) {
	// TODO: we should probably let the caller do this.
	logger := zap.NewNop()
	logger.Core().Enabled(zap.PanicLevel)
	bgCtx = logctx.NewContext(bgCtx, logger)

	return bclocal.New(bclocal.Env{
		Background: bgCtx,
		StateDir:   stateDir,
		PrivateKey: privKey,
		MkSchema:   mkSchema,
		Root:       reposchema.GotRepoVolumeSpec(),
		Policy:     pol,
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

var _ bclocal.Policy = &bcPolicy{}

type bcPolicy struct {
	bgCtx context.Context
	root  *os.Root

	sem               semaphore.Weighted
	mu                sync.RWMutex
	canLook, canTouch []inet256.Addr
	lastReload        time.Time
}

func newBCPolicy(root *os.Root, bgCtx context.Context) *bcPolicy {
	return &bcPolicy{
		bgCtx: bgCtx,
		root:  root,
		sem:   *semaphore.NewWeighted(1),
	}
}

func (pol *bcPolicy) reload(ctx context.Context) error {
	if err := pol.sem.Acquire(ctx, 1); err != nil {
		return err
	}
	defer pol.sem.Release(1)
	cfg, err := LoadConfig(pol.root)
	if err != nil {
		return err
	}
	pol.mu.Lock()
	defer pol.mu.Unlock()
	pol.canLook = cfg.Blobcache.InProcess.CanLook
	pol.canTouch = cfg.Blobcache.InProcess.CanTouch
	pol.lastReload = time.Now()
	return nil
}

func (pol *bcPolicy) get(ctx context.Context) (canLook, canTouch []inet256.Addr) {
	pol.mu.RLock()
	needReload := time.Since(pol.lastReload) > time.Second
	pol.mu.RUnlock()
	if needReload {
		if err := pol.reload(ctx); err != nil {
			logctx.Error(ctx, "failed to reload blobcache policy from config", zap.Error(err))
		}
	}

	pol.mu.RLock()
	defer pol.mu.RUnlock()
	return pol.canLook, pol.canTouch
}

func (pol *bcPolicy) CanConnect(x blobcache.PeerID) bool {
	canLook, canTouch := pol.get(pol.bgCtx)
	for _, list := range [][]inet256.Addr{canLook, canTouch} {
		if slices.Contains(list, x) {
			return true
		}
	}
	return false
}

func (pol *bcPolicy) CanCreate(x blobcache.PeerID) bool {
	return false
}

func (pol *bcPolicy) OpenFiat(peer blobcache.PeerID, target blobcache.OID) blobcache.ActionSet {
	if target == (blobcache.OID{}) {
		// don't allow any access to the root
		return 0
	}

	const roAct = blobcache.Action_ACK |
		blobcache.Action_VOLUME_BEGIN_TX |
		blobcache.Action_VOLUME_INSPECT |
		blobcache.Action_TX_INSPECT |
		blobcache.Action_TX_EXISTS
	const rwAct = roAct |
		blobcache.Action_TX_POST |
		blobcache.Action_TX_SAVE

	canLook, canTouch := pol.get(pol.bgCtx)
	if slices.Contains(canTouch, peer) {
		return rwAct
	}
	if slices.Contains(canLook, peer) {
		return roAct
	}
	return 0
}
