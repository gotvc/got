// Package gotwc implements working copies of Got repositories
package gotwc

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/exp/slices2"
	"go.brendoncarroll.net/state/posixfs"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv/kvstreams"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/gotwc/internal/porting"
	"github.com/gotvc/got/src/gotwc/internal/staging"
	"github.com/gotvc/got/src/internal/gotbc"
	"github.com/gotvc/got/src/internal/gotcore"
)

const (
	configPath = ".got/wc-config"
	dbPath     = ".got/wc.db"

	defaultFileMode = 0o644
	defaultDirMode  = 0o755
	nameMaster      = "master"
)

func IsWC(wcRoot *os.Root) (bool, error) {
	_, err := LoadConfig(wcRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Init initializes a new working copy in wcdir
// The working copy will be associated with the given repo.
// cfg.RepoDir will be overriden with repo.Dir().
func Init(wcRoot *os.Root, cfg Config) error {
	if err := wcRoot.MkdirAll(".got", defaultDirMode); err != nil {
		return err
	}
	return SaveConfig(wcRoot, cfg)
}

// Open opens a directory as a WorkingCopy
// `wcdir` is a directory containing a .got/wc-config file
func Open(root *os.Root) (_ *WC, retErr error) {
	cfg, err := LoadConfig(root)
	if err != nil {
		return nil, err
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

	var closers []func() error

	// blobcache
	bc, err := gotbc.OpenBlobcache(root, cfg.Blobcache, bgCtx)
	if err != nil {
		return nil, err
	}
	if srv, ok := bc.(*bclocal.Service); ok {
		closers = append(closers, srv.Close)
	}

	// now check for a repo config, if there is one, then we set repoDir
	var repoDir *os.Root
	if _, err := gotrepo.LoadConfig(root); err != nil && !os.IsNotExist(err) {
		return nil, err
	} else if err == nil {
		repoDir = root
	}

	repo, err := gotrepo.Open(bgCtx, bc, cfg.Repo, repoDir)
	if err != nil {
		return nil, err
	}
	wc, err := New(repo, root)
	if err != nil {
		return nil, err
	}
	wc.closers = closers
	return wc, nil
}

// New creates a new working copy using root and the Repo
func New(repo *gotrepo.Repo, root *os.Root) (*WC, error) {
	db, err := bbolt.Open(filepath.Join(root.Name(), dbPath), 0o600, nil)
	if err != nil {
		return nil, err
	}
	cfg, err := LoadConfig(root)
	if err != nil {
		db.Close()
		return nil, err
	}
	return &WC{
		root: root,
		repo: repo,
		id:   cfg.ID,

		fsys: posixfs.NewDirFS(root.Name()),
		db:   db,
	}, nil
}

// WC is a Working Copy
// A WC is a directory in the local filesystem, which has a .got/wc.db file
// WC are associated with a single repository each.
type WC struct {
	root                  *os.Root
	repo                  *gotrepo.Repo
	closeBlobcacheOnClose bool
	closeAll              bool
	id                    gotrepo.WorkingCopyID

	// TODO: eventually we should move away from this interface, but
	// the existing importers and exporters use it.
	fsys    posixfs.FS
	db      *bbolt.DB
	closers []func() error
}

func (wc *WC) Repo() *gotrepo.Repo {
	return wc.repo
}

func (wc *WC) Dir() string {
	return wc.root.Name()
}

func (wc *WC) Blobcache() blobcache.Service {
	return wc.Repo().Blobcache()
}

func (wc *WC) Configure(ctx context.Context, fn func(Config) Config) error {
	var cfg2 Config
	if err := EditConfig(wc.root, func(x Config) Config {
		x = fn(x)
		cfg2 = x
		return x
	}); err != nil {
		return err
	}
	if bc, ok := wc.Blobcache().(*gotbc.Local); ok {
		bc.SetPolicy(cfg2.Blobcache.InProcess.CanLook, cfg2.Blobcache.InProcess.CanTouch)
	}
	return nil
}

// ListSpans returns the tracked spans
func (wc *WC) ListSpans(ctx context.Context) ([]Span, error) {
	cfg, err := LoadConfig(wc.root)
	if err != nil {
		return nil, err
	}
	return slices2.Map(cfg.Tracking, func(p string) Span {
		return PrefixSpan(p)
	}), nil
}

// Track causes the working copy to include another range of files.
func (wc *WC) Track(ctx context.Context, span Span) error {
	if !span.IsPrefix() {
		return fmt.Errorf("only prefix spans are supported")
	}
	newPrefix := span.Begin
	return EditConfig(wc.root, func(x Config) Config {
		x.Tracking = append(x.Tracking, newPrefix)
		x.Tracking = compactPrefixes(x.Tracking)
		return x
	})
}

// Untrack causes the working copy to exclude a range of files.
func (wc *WC) Untrack(ctx context.Context, span Span) error {
	if !span.IsPrefix() {
		return fmt.Errorf("only prefix spans are supported")
	}
	delPrefix := span.Begin
	return EditConfig(wc.root, func(x Config) Config {
		x.Tracking = slices.DeleteFunc(x.Tracking, func(p string) bool {
			return p == delPrefix
		})
		return x
	})
}

func (wc *WC) GetSaveTo() (string, error) {
	cfg, err := LoadConfig(wc.root)
	if err != nil {
		return "", err
	}
	return cfg.SaveTo, nil
}

func (wc *WC) GetBase() ([]gdat.Ref, error) {
	cfg, err := LoadConfig(wc.root)
	if err != nil {
		return nil, err
	}
	return cfg.Base, nil
}

// SetHead sets HEAD to name
// SetHead will check if the staging area is not empty, and if it's not
// the it will check that the desired branch has the same content parameters
// as the current branch.
// If one branch is a fork of another, or they have a common ancestor somewhere,
// the it is very likely that they have the same content parameters.
func (wc *WC) SetHead(ctx context.Context, name string) error {
	desiredInfo, err := wc.repo.InspectMark(ctx, gotrepo.FQM{Name: name})
	if err != nil {
		return err
	}
	baseRef, _, err := wc.repo.MarkLoadCommit(ctx, gotrepo.FQM{Name: name})
	if err != nil {
		return err
	}
	activeName, err := wc.GetSaveTo()
	if err != nil {
		return err
	}
	activeInfo, err := wc.repo.InspectMark(ctx, gotrepo.FQM{Name: activeName})
	if err != nil && !gotcore.IsNotExist(err) {
		return err
	}
	if activeInfo != nil {
		if desiredInfo.Config.Hash() != activeInfo.Config.Hash() {
			stagetx, err := wc.beginStageTx(ctx, nil, false)
			if err != nil {
				return err
			}
			defer stagetx.Abort(ctx)
			isEmpty, err := stagetx.IsEmpty(ctx)
			if err != nil {
				return err
			}
			if !isEmpty {
				return fmt.Errorf("staging must be empty to change to a branch with a different salt")
			}
		}
	}
	var baseRefs []gdat.Ref
	if !baseRef.IsZero() {
		baseRefs = append(baseRefs, baseRef)
	}
	return wc.setHead(name, baseRefs)
}

func (wc *WC) setHead(branchName string, base []gdat.Ref) error {
	return EditConfig(wc.root, func(x Config) Config {
		x.SaveTo = branchName
		x.Base = base
		return x
	})
}

func (wc *WC) GetActAs() (string, error) {
	cfg, err := LoadConfig(wc.root)
	if err != nil {
		return "", err
	}
	return cfg.ActAs, nil
}

func (wc *WC) SetActAs(idenName string) error {
	return EditConfig(wc.root, func(x Config) Config {
		x.ActAs = idenName
		return x
	})
}

func (wc *WC) beginStageTx(ctx context.Context, paramHash *[32]byte, modify bool) (*staging.Tx, error) {
	tx, err := wc.repo.BeginStagingTx(ctx, wc.id, modify)
	if err != nil {
		return nil, err
	}
	if paramHash == nil && modify {
		return nil, fmt.Errorf("paramHash must be provided for modifying transaction.")
	}
	kvmach := staging.DefaultGotKV()
	return staging.New(&kvmach, tx, paramHash), nil
}

// StageIsEmpty returns (true, nil) IFF there are no changes staged.
func (wc *WC) StageIsEmpty(ctx context.Context) (bool, error) {
	var notEmpty bool
	if err := wc.ForEachStaging(ctx, func(p string, op FileOperation) error {
		notEmpty = true
		return fmt.Errorf("stop iter")
	}); err != nil {
		if notEmpty {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Export overwrites data in the filesystem with data from the Commit at HEAD.
// Only tracked paths are overwritten.
func (wc *WC) Export(ctx context.Context) error {
	if emptyStage, err := wc.StageIsEmpty(ctx); err != nil {
		return err
	} else if !emptyStage {
		// we may be able to remove this
		return fmt.Errorf("cannot export to filesystem, staging area must be empty (it's not)")
	}
	mname, err := wc.GetSaveTo()
	if err != nil {
		return nil
	}
	return wc.repo.ViewMark(ctx, gotrepo.FQM{Name: mname}, func(mtx *gotcore.MarkTx) error {
		paramHash := mtx.Config().Hash()
		portDB := porting.NewDB(wc.db, paramHash)
		fsys, filter, err := wc.getFilteredFS(ctx)
		if err != nil {
			return err
		}
		var root gotfs.Root
		if ok, err := mtx.LoadFS(ctx, &root); err != nil {
			return err
		} else if !ok {
			logctx.Warnf(ctx, "mark does not have a commit, nothing to export")
			return nil
		}
		exp := porting.NewExporter(mtx.GotFS(), portDB, fsys, filter)
		ss := mtx.FSRO()
		return exp.ExportPath(ctx, ss, root, "")
	})
}

func (wc *WC) Clobber(ctx context.Context, p string) error {
	mname, err := wc.GetSaveTo()
	if err != nil {
		return nil
	}
	return wc.repo.ViewMark(ctx, gotrepo.FQM{Name: mname}, func(mtx *gotcore.MarkTx) error {
		paramHash := mtx.Config().Hash()
		fsys, filter, err := wc.getFilteredFS(ctx)
		if err != nil {
			return err
		}
		portDB := porting.NewDB(wc.db, paramHash)
		exp := porting.NewExporter(mtx.GotFS(), portDB, fsys, filter)
		ss := mtx.FSRO()
		var root gotfs.Root
		if ok, err := mtx.LoadFS(ctx, &root); err != nil {
			return err
		} else if !ok {
			logctx.Warnf(ctx, "mark has no commit, nothing to clobber")
			return nil
		}
		return exp.Clobber(ctx, ss, root, p)
	})
}

// Checkout sets HEAD, and then performs an Export of all tracked spans.
func (wc *WC) Checkout(ctx context.Context, name string) error {
	if err := wc.SetHead(ctx, name); err != nil {
		return err
	}
	return wc.Export(ctx)
}

// Fork calls CloneMark on the repo with the current head, creating a new
// mark `next`.
// Then the WC's head is set to next.
func (wc *WC) Fork(ctx context.Context, next string) error {
	head, err := wc.GetSaveTo()
	if err != nil {
		return err
	}
	if err := wc.repo.CloneMark(ctx, gotrepo.FQM{Name: head}, gotrepo.FQM{Name: next}); err != nil {
		return err
	}
	if err := wc.SetHead(ctx, next); err != nil {
		return err
	}
	return nil
}

func (wc *WC) Close() error {
	if err := wc.db.Close(); err != nil {
		return err
	}
	for _, closer := range wc.closers {
		if err := closer(); err != nil {
			return err
		}
	}
	return nil
}

// Cleanup removes unreferenced data from the stage.
func (wc *WC) Cleanup(ctx context.Context) error {
	logctx.Infof(ctx, "removing blobs from staging areas")
	return wc.cleanupStagingBlobs(ctx)
}

func (wc *WC) viewSnap(ctx context.Context, fn func(*gotcore.ViewCtx) error) error {
	mname, err := wc.GetSaveTo()
	if err != nil {
		return nil
	}
	return wc.repo.ViewCommit(ctx, &gotcore.CommitExpr_Mark{Name: mname}, fn)
}

func (wc *WC) filter(spans []Span) func(p string) bool {
	return func(x string) bool {
		if strings.HasPrefix(x, ".got") {
			return false
		}
		return spansContain(spans, x)
	}
}

func (wc *WC) getFilteredFS(ctx context.Context) (posixfs.FS, func(string) bool, error) {
	spans, err := wc.ListSpans(ctx)
	if err != nil {
		return nil, nil, err
	}
	filter := wc.filter(spans)
	return posixfs.NewFiltered(wc.fsys, filter), filter, nil
}

type Span = porting.Span

func PrefixSpan(prefix string) Span {
	return Span{
		Begin: prefix,
		End:   string(kvstreams.PrefixEnd([]byte(prefix))),
	}
}

func spansContain(spans []Span, x string) bool {
	for _, span := range spans {
		if span.Contains(x) {
			return true
		}
	}
	return false
}

func compactPrefixes(xs []string) []string {
	slices.Sort(xs)
	return slices.Compact(xs)
}

type ErrWouldClobber = porting.ErrWouldClobber
