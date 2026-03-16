package gotrepo

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/bchttp"
	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"github.com/cloudflare/circl/sign/ed25519"
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
	// Client uses the Client Blobcache service, as configured through BLOBCACHE_API
	EnvClient *EnvClientBlobcache `json:"env_client,omitempty"`
}

type InProcessBlobcache struct {
	ActAs    string       `json:"act_as"`
	CanLook  []inet256.ID `json:"can_look"`
	CanTouch []inet256.ID `json:"can_touch"`
}

// EnvBlobcache configures blobcache to create a client using the environment variable
type EnvClientBlobcache struct {
	UseSchema bool `json:"use_schema,omitempty"`
}

func newBCInfoLogger() *zap.Logger {
	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(zap.PanicLevel)
	l, _ := cfg.Build()
	return l
}

func makeBlobcache(repo *os.Root, config Config, spec BlobcacheSpec, bgCtx context.Context) (blobcache.Service, error) {
	var svc blobcache.Service
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
		svc = bc
	case spec.EnvClient != nil:
		svc = bcclient.NewClientFromEnv()
	default:
		return nil, fmt.Errorf("empty blobcache spec: %v", spec)
	}
	return svc, nil
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
	return bcclient.NewClient(u).(*bchttp.Client), nil
}

func mkSchema(spec blobcache.SchemaSpec) (schema.Schema, error) {
	switch spec.Name {
	case reposchema.SchemaName_GotRepo:
		return reposchema.Constructor(spec.Params, nil)
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

var _ blobcache.Service = &bcIntercept{}

type bcIntercept struct {
	svc    blobcache.Service
	logger *zap.Logger
}

func (b *bcIntercept) ctx(ctx context.Context) context.Context {
	return logctx.NewContext(ctx, b.logger)
}

func (b *bcIntercept) Endpoint(ctx context.Context) (blobcache.Endpoint, error) {
	return b.svc.Endpoint(b.ctx(ctx))
}

func (b *bcIntercept) Drop(ctx context.Context, h blobcache.Handle) error {
	return b.svc.Drop(b.ctx(ctx), h)
}

func (b *bcIntercept) KeepAlive(ctx context.Context, hs []blobcache.Handle) error {
	return b.svc.KeepAlive(b.ctx(ctx), hs)
}

func (b *bcIntercept) InspectHandle(ctx context.Context, h blobcache.Handle) (*blobcache.HandleInfo, error) {
	return b.svc.InspectHandle(b.ctx(ctx), h)
}

func (b *bcIntercept) Share(ctx context.Context, h blobcache.Handle, to blobcache.PeerID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	return b.svc.Share(b.ctx(ctx), h, to, mask)
}

func (b *bcIntercept) CreateVolume(ctx context.Context, host *blobcache.Endpoint, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	return b.svc.CreateVolume(b.ctx(ctx), host, vspec)
}

func (b *bcIntercept) InspectVolume(ctx context.Context, h blobcache.Handle) (*blobcache.VolumeInfo, error) {
	return b.svc.InspectVolume(b.ctx(ctx), h)
}

func (b *bcIntercept) OpenFiat(ctx context.Context, x blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	return b.svc.OpenFiat(b.ctx(ctx), x, mask)
}

func (b *bcIntercept) OpenFrom(ctx context.Context, base blobcache.Handle, ltok blobcache.LinkToken, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	return b.svc.OpenFrom(b.ctx(ctx), base, ltok, mask)
}

func (b *bcIntercept) BeginTx(ctx context.Context, volh blobcache.Handle, txp blobcache.TxParams) (*blobcache.Handle, error) {
	return b.svc.BeginTx(b.ctx(ctx), volh, txp)
}

func (b *bcIntercept) CloneVolume(ctx context.Context, caller *blobcache.PeerID, volh blobcache.Handle) (*blobcache.Handle, error) {
	return b.svc.CloneVolume(b.ctx(ctx), caller, volh)
}

func (b *bcIntercept) InspectTx(ctx context.Context, tx blobcache.Handle) (*blobcache.TxInfo, error) {
	return b.svc.InspectTx(b.ctx(ctx), tx)
}

func (b *bcIntercept) Commit(ctx context.Context, tx blobcache.Handle) error {
	return b.svc.Commit(b.ctx(ctx), tx)
}

func (b *bcIntercept) Abort(ctx context.Context, tx blobcache.Handle) error {
	return b.svc.Abort(b.ctx(ctx), tx)
}

func (b *bcIntercept) Load(ctx context.Context, tx blobcache.Handle, dst *[]byte) error {
	return b.svc.Load(b.ctx(ctx), tx, dst)
}

func (b *bcIntercept) Save(ctx context.Context, tx blobcache.Handle, src []byte) error {
	return b.svc.Save(b.ctx(ctx), tx, src)
}

func (b *bcIntercept) Post(ctx context.Context, tx blobcache.Handle, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	return b.svc.Post(b.ctx(ctx), tx, data, opts)
}

func (b *bcIntercept) Get(ctx context.Context, tx blobcache.Handle, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
	return b.svc.Get(b.ctx(ctx), tx, cid, buf, opts)
}

func (b *bcIntercept) Exists(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	return b.svc.Exists(b.ctx(ctx), tx, cids, dst)
}

func (b *bcIntercept) Delete(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID) error {
	return b.svc.Delete(b.ctx(ctx), tx, cids)
}

func (b *bcIntercept) Copy(ctx context.Context, tx blobcache.Handle, srcTxns []blobcache.Handle, cids []blobcache.CID, success []bool) error {
	return b.svc.Copy(b.ctx(ctx), tx, srcTxns, cids, success)
}

func (b *bcIntercept) Visit(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID) error {
	return b.svc.Visit(b.ctx(ctx), tx, cids)
}

func (b *bcIntercept) IsVisited(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID, yesVisited []bool) error {
	return b.svc.IsVisited(b.ctx(ctx), tx, cids, yesVisited)
}

func (b *bcIntercept) Link(ctx context.Context, tx blobcache.Handle, target blobcache.Handle, mask blobcache.ActionSet) (*blobcache.LinkToken, error) {
	return b.svc.Link(b.ctx(ctx), tx, target, mask)
}

func (b *bcIntercept) Unlink(ctx context.Context, tx blobcache.Handle, ltoks []blobcache.LinkToken) error {
	return b.svc.Unlink(b.ctx(ctx), tx, ltoks)
}

func (b *bcIntercept) VisitLinks(ctx context.Context, tx blobcache.Handle, targets []blobcache.LinkToken) error {
	return b.svc.VisitLinks(b.ctx(ctx), tx, targets)
}

func (b *bcIntercept) CreateQueue(ctx context.Context, host *blobcache.Endpoint, qspec blobcache.QueueSpec) (*blobcache.Handle, error) {
	return b.svc.CreateQueue(b.ctx(ctx), host, qspec)
}

func (b *bcIntercept) InspectQueue(ctx context.Context, qh blobcache.Handle) (blobcache.QueueInfo, error) {
	return b.svc.InspectQueue(b.ctx(ctx), qh)
}

func (b *bcIntercept) Dequeue(ctx context.Context, q blobcache.Handle, buf []blobcache.Message, opts blobcache.DequeueOpts) (int, error) {
	return b.svc.Dequeue(b.ctx(ctx), q, buf, opts)
}

func (b *bcIntercept) Enqueue(ctx context.Context, q blobcache.Handle, msgs []blobcache.Message) (*blobcache.InsertResp, error) {
	return b.svc.Enqueue(b.ctx(ctx), q, msgs)
}

func (b *bcIntercept) SubToVolume(ctx context.Context, q blobcache.Handle, vol blobcache.Handle, spec blobcache.VolSubSpec) error {
	return b.svc.SubToVolume(b.ctx(ctx), q, vol, spec)
}
