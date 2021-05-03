package got

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/s/peerswarm"
	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/fs"
	"github.com/brendoncarroll/got/pkg/gdat"
	"github.com/brendoncarroll/got/pkg/gotfs"
	"github.com/brendoncarroll/got/pkg/gotnet"
	"github.com/brendoncarroll/got/pkg/gotvc"
	"github.com/brendoncarroll/got/pkg/volumes"
	"github.com/inet256/inet256/pkg/inet256p2p"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

// default bucket
const (
	bucketDefault = "default"
	keyStaging    = "STAGING"
	keyActive     = "ACTIVE"
	nameMaster    = "master"
)

const (
	bucketCellData = "cells"
	bucketStores   = "stores"
	bucketTracker  = "tracker"
)

// fs paths
const (
	gotPrefix      = ".got"
	configPath     = ".got/config"
	privateKeyPath = ".got/private.pem"
	specDirPath    = ".got/volume_specs"
	policyPath     = ".got/policy"
	storePath      = ".got/blobs"
)

type Repo struct {
	rootPath   string
	repoFS     FS
	db         *bolt.DB
	config     Config
	policy     *Policy
	privateKey p2p.PrivateKey

	realms       []Realm
	workingDir   FS
	specDir      *volSpecDir
	storeManager *storeManager
	tracker      *tracker
	swarm        peerswarm.AskSwarm
}

func InitRepo(p string) error {
	repoDirFS := fs.NewDirFS(p)
	if err := repoDirFS.Mkdir(gotPrefix, 0o755); err != nil {
		return err
	}
	if err := repoDirFS.Mkdir(specDirPath, 0o755); err != nil {
		return err
	}
	if _, err := repoDirFS.Stat(configPath); os.IsNotExist(err) {
	} else if err != nil {
		return err
	} else {
		return errors.Errorf("repo already exists")
	}
	config := DefaultConfig()
	if err := SaveConfig(repoDirFS, configPath, config); err != nil {
		return err
	}
	privKey := generatePrivateKey()
	if err := SavePrivateKey(repoDirFS, privateKeyPath, privKey); err != nil {
		return err
	}
	if err := fs.WriteIfNotExists(repoDirFS, policyPath, nil); err != nil {
		return err
	}
	r, err := OpenRepo(p)
	if err != nil {
		return err
	}
	return r.Close()
}

func OpenRepo(p string) (*Repo, error) {
	repoFS := fs.NewDirFS(p)
	config, err := LoadConfig(repoFS, configPath)
	if err != nil {
		return nil, err
	}
	db, err := bolt.Open(dbPath(p), 0o644, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	privateKey, err := LoadPrivateKey(repoFS, privateKeyPath)
	if err != nil {
		return nil, err
	}
	r := &Repo{
		rootPath:   p,
		repoFS:     repoFS,
		config:     *config,
		privateKey: privateKey,
		db:         db,
		workingDir: fs.NewFilterFS(repoFS, func(x string) bool {
			return !strings.HasPrefix(x, gotPrefix)
		}),
		tracker: newTracker(db, []string{bucketTracker}),
	}
	r.specDir = newVolSpecDir(r.MakeCell, r.MakeStore, fs.NewDirFS(filepath.Join(r.rootPath, specDirPath)))
	if err := volumes.CreateIfNotExists(context.TODO(), r.specDir, nameMaster); err != nil {
		return nil, err
	}
	fsStore := cadata.NewFSStore(fs.NewDirFS(filepath.Join(r.rootPath, storePath)))
	r.storeManager = newStoreManager(fsStore, r.db, bucketStores)
	return r, nil
}

func (r *Repo) Close() (retErr error) {
	for _, fn := range []func() error{
		r.db.Close,
	} {
		if err := fn(); retErr == nil {
			retErr = err
		}
	}
	return retErr
}

func (r *Repo) WorkingDir() FS {
	return r.workingDir
}

func (r *Repo) GetACL() gotnet.ACL {
	return r.policy
}

func (r *Repo) GetRealm() Realm {
	return volumes.NewLayered(append(r.realms, r.specDir)...)
}

func (r *Repo) getSwarm() (peerswarm.AskSwarm, error) {
	if r.swarm != nil {
		return r.swarm, nil
	}
	swarm, err := inet256p2p.NewSwarm("127.0.0.1:25600", r.privateKey)
	if err != nil {
		return nil, err
	}
	r.swarm = swarm
	return swarm, nil
}

func (r *Repo) getFSOp() *gotfs.Operator {
	o := gotfs.NewOperator()
	return &o
}

func (r *Repo) getDataOp() *gdat.Operator {
	o := gdat.NewOperator()
	return &o
}

func dbPath(x string) string {
	return filepath.Join(x, gotPrefix, "local.db")
}

func (r *Repo) Log(ctx context.Context, fn func(ref Ref, s Commit) error) error {
	_, vol, err := r.GetActiveVolume(ctx)
	if err != nil {
		return err
	}
	snap, err := getSnapshot(ctx, vol.Cell)
	if err != nil {
		return err
	}
	if snap == nil {
		return nil
	}
	return gotvc.ForEachAncestor(ctx, vol.VCStore, *snap, fn)
}

func (r *Repo) DebugDB(ctx context.Context, w io.Writer) error {
	return r.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketDefault))
		if b == nil {
			return nil
		}
		fmt.Fprintf(w, "BUCKET: %s\n", bucketDefault)
		dumpBucket(w, b)

		b = tx.Bucket([]byte(bucketCellData))
		if b == nil {
			return nil
		}
		fmt.Fprintf(w, "BUCKET: %s\n", bucketCellData)
		dumpBucket(w, b)

		return nil
	})
}

func dumpBucket(w io.Writer, b *bolt.Bucket) {
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		fmt.Fprintf(w, "%q -> %q\n", k, v)
	}
	fmt.Fprintln(w)
}
