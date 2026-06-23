// package gotbc manages setting up Blobcache for Got.
// Blobcache can either be run in process, or connected to using an environment variable
package gotbc

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/bchttp"
	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"github.com/cloudflare/circl/sign/ed25519"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/internal/stores"
	"go.inet256.org/inet256/src/inet256"
	"go.uber.org/zap"
)

const (
	blobcacheStatePath = ".got/blobcache"

	// privateSeedPath is the path to the private seed *within* the stateDir
	privateSeedPath = "PRIVATE"
)

// Config describes how to access a Blobcache Service.
type Config struct {
	// InProcess uses an in-process Blobcache service.
	// The state will be stored in the .got/blobcache directory.
	// This is the default.
	// The state can get quite large for large datasets, so it is recommended to use the system's Blobcache.
	InProcess *InProcessSpec `json:"in_process,omitempty"`
	// Client uses the Client Blobcache service, as configured through BLOBCACHE_API
	EnvClient *EnvClientSpec `json:"env_client,omitempty"`
}

type InProcessSpec struct {
	// Path is the path to the directory containing .got/blobcache
	// If empty then the current got root directory is used.
	Path     string       `json:"path"`
	CanLook  []inet256.ID `json:"can_look"`
	CanTouch []inet256.ID `json:"can_touch"`
}

// EnvBlobcache configures blobcache to create a client using the environment variable
type EnvClientSpec struct {
	UseSchema bool `json:"use_schema,omitempty"`
}

// GotVolumeSpec returns the default VolumeSpec for all Got Volumes.
// It uses BLAKE2b and has a MaxSize of 1 << 21
func GotVolumeSpec() blobcache.VolumeSpec {
	return blobcache.VolumeSpec{
		Local: &blobcache.VolumeBackend_Local{
			HashAlgo: stores.HashAlgo,
			MaxSize:  stores.MaxSize,
			Salted:   false,
		},
	}
}

func newBCLogger() *zap.Logger {
	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	l, _ := cfg.Build()
	return l
}

// OpenBlobcache returns a blobcache client according to the spec.
// dir should be the directory containing the .got directory.
func OpenBlobcache(wcDir *os.Root, spec Config, bgCtx context.Context) (blobcache.Service, error) {
	var svc blobcache.Service
	switch {
	case spec.InProcess != nil:
		spec2 := *spec.InProcess
		var stateDir string
		if spec2.Path == "" {
			if err := wcDir.MkdirAll(blobcacheStatePath, 0o755); err != nil {
				return nil, err
			}
			stateDir = filepath.Join(wcDir.Name(), blobcacheStatePath)
		} else {
			var err error
			stateDir, err = filepath.Abs(spec.InProcess.Path)
			if err != nil {
				return nil, err
			}
		}
		stateRoot, err := os.OpenRoot(stateDir)
		if err != nil {
			return nil, err
		}
		pol := new(bcPolicy)
		bc, err := OpenLocal(stateRoot, bgCtx, pol)
		if err != nil {
			return nil, err
		}
		svc = &Local{svc: bc, logger: newBCLogger(), pol: pol}
	case spec.EnvClient != nil:
		svc = bcclient.NewClientFromEnv()
	default:
		return nil, fmt.Errorf("empty blobcache spec: %v", spec)
	}
	return svc, nil
}

// OpenLocal runs a new blobcache Node using stateDir for all of the state.
func OpenLocal(stateDir *os.Root, bgCtx context.Context, pol bclocal.Policy) (*bclocal.Service, error) {
	privKey, err := ensureBlobcachePrivateKey(stateDir)
	if err != nil {
		return nil, err
	}
	return bclocal.New(bclocal.Env{
		Background: bgCtx,
		StateDir:   stateDir.Name(),
		PrivateKey: privKey,
		MkSchema:   MkSchema,
		Root:       GotVolumeSpec(),
		Policy:     pol,
	}, bclocal.Config{})
}

func openHTTPBlobcache(u string) (*bchttp.Client, error) {
	return bcclient.NewClient(u).(*bchttp.Client), nil
}

func ensureBlobcachePrivateKey(stateDir *os.Root) (ed25519.PrivateKey, error) {
	privSeed, err := ensureBlobcachePrivateSeed(stateDir)
	if err != nil {
		return nil, err
	}
	// derive an ed25519 key
	var keySeed [32]byte
	gdat.DeriveKey(keySeed[:], (*[32]byte)(&privSeed), []byte("ed25519"))
	privKey := ed25519.NewKeyFromSeed(keySeed[:])
	return privKey, nil
}

// ensureBlobcachePrivateSeed retreives or creates the blobcache secret
func ensureBlobcachePrivateSeed(stateDir *os.Root) ([32]byte, error) {
	// check if it already exists
	data, err := stateDir.ReadFile(privateSeedPath)
	if err != nil && !os.IsNotExist(err) {
		return [32]byte{}, err
	}
	if len(data) > 0 && len(data) != 32 {
		return [32]byte{}, fmt.Errorf("invalid blobcache private seed len=%d", len(data))
	} else if len(data) == 32 {
		return [32]byte(data), nil
	}
	// exclusively create the file.
	f, err := stateDir.OpenFile(privateSeedPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return [32]byte{}, err
	}
	defer f.Close()
	var secret [32]byte
	rand.Read(secret[:])
	_, err = f.WriteAt(secret[:], 0)
	if err != nil {
		return [32]byte{}, err
	}
	return secret, f.Sync()
}

func NewTest(t testing.TB) blobcache.Service {
	env := bclocal.NewTestEnv(t)
	env.Root = GotVolumeSpec()
	return bclocal.NewTestServiceFromEnv(t, env)
}

type PolicyFunc = func(context.Context) (canLook, canTouch []inet256.ID, _ error)

var _ bclocal.Policy = &bcPolicy{}

type bcPolicy struct {
	mu                sync.RWMutex
	canLook, canTouch []inet256.ID
}

func (pol *bcPolicy) Update(canLook, canTouch []inet256.ID) {
	pol.mu.Lock()
	defer pol.mu.Unlock()
	pol.canLook = canLook
	pol.canTouch = canTouch
}

func (pol *bcPolicy) get() (canLook, canTouch []inet256.Addr) {
	pol.mu.RLock()
	defer pol.mu.RUnlock()
	return pol.canLook, pol.canTouch
}

func (pol *bcPolicy) CanConnect(x blobcache.NodeID) bool {
	canLook, canTouch := pol.get()
	for _, list := range [][]inet256.Addr{canLook, canTouch} {
		if slices.Contains(list, x) {
			return true
		}
	}
	return false
}

func (pol *bcPolicy) CanCreate(x blobcache.NodeID) bool {
	return false
}

func (pol *bcPolicy) OpenFiat(peer blobcache.NodeID, target blobcache.OID) blobcache.ActionSet {
	if target == (blobcache.OID{}) {
		// don't allow any access to the root
		return 0
	}

	const roAct = blobcache.Action_ACK |
		blobcache.Action_VOLUME_INSPECT |
		blobcache.Action_VOLUME_BEGIN_TX |
		blobcache.Action_VOLUME_TX_INSPECT |
		blobcache.Action_VOLUME_TX_LOAD |
		blobcache.Action_VOLUME_TX_GET |
		blobcache.Action_VOLUME_TX_EXISTS |
		blobcache.Action_VOLUME_TX_COPY_FROM
	const rwAct = roAct |
		blobcache.Action_VOLUME_LINK_TO |
		blobcache.Action_VOLUME_TX_SAVE |
		blobcache.Action_VOLUME_TX_POST |
		blobcache.Action_VOLUME_TX_DELETE |
		blobcache.Action_VOLUME_TX_COPY_TO |
		blobcache.Action_VOLUME_TX_LINK_FROM |
		blobcache.Action_VOLUME_TX_UNLINK_FROM |
		blobcache.Action_VOLUME_TX_VISIT |
		blobcache.Action_VOLUME_TX_IS_VISITED |
		blobcache.Action_VOLUME_TX_VISIT_LINKS

	canLook, canTouch := pol.get()
	if slices.Contains(canTouch, peer) {
		return rwAct
	}
	if slices.Contains(canLook, peer) {
		return roAct
	}
	return 0
}
