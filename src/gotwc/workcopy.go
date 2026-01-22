// Package gotwc implements working copies of Got repositories
package gotwc

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv/kvstreams"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/gotwc/internal/dbmig"
	"github.com/gotvc/got/src/gotwc/internal/migrations"
	"github.com/gotvc/got/src/gotwc/internal/porting"
	"github.com/gotvc/got/src/gotwc/internal/sqlutil"
	"github.com/gotvc/got/src/gotwc/internal/staging"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/slices2"
	"go.brendoncarroll.net/state/posixfs"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.brendoncarroll.net/tai64"
)

const (
	configPath = ".got/wc-config"
	dbPath     = ".got/wc.db"

	defaultFileMode = 0o644
	defaultDirMode  = 0o755
	nameMaster      = "master"
)

// Init initializes a new working copy in wcdir
// The working copy will be associated with the given repo.
// cfg.RepoDir will be overriden with repo.Dir().
func Init(repo *gotrepo.Repo, wcRoot *os.Root, cfg Config) error {
	if err := wcRoot.MkdirAll(".got", defaultDirMode); err != nil {
		return err
	}
	cfg.RepoDir = repo.Dir()
	return SaveConfig(wcRoot, cfg)
}

// Open opens a directory as a WorkingCopy
// `wcdir` is a directory containing a .got/wc-config file
func Open(root *os.Root) (*WC, error) {
	cfg, err := LoadConfig(root)
	if err != nil {
		return nil, err
	}
	repoRoot, err := os.OpenRoot(cfg.RepoDir)
	if err != nil {
		return nil, err
	}
	repo, err := gotrepo.Open(repoRoot)
	if err != nil {
		return nil, err
	}
	wc, err := New(repo, root)
	if err != nil {
		return nil, err
	}
	wc.closeRepoOnClose = true
	return wc, nil
}

// New creates a new working copy using root and the Repo
func New(repo *gotrepo.Repo, root *os.Root) (*WC, error) {
	p := root.Name()
	db, err := sqlutil.OpenPool(filepath.Join(p, dbPath))
	if err != nil {
		return nil, err
	}
	ctx := context.TODO()
	// setup database
	if err := sqlutil.Borrow(ctx, db, func(conn *sqlutil.Conn) error {
		if err := migrations.EnsureAll(conn, dbmig.ListMigrations()); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	cfg, err := LoadConfig(root)
	if err != nil {
		return nil, err
	}
	return &WC{
		root: root,
		repo: repo,
		id:   cfg.ID,

		fsys: posixfs.NewDirFS(p),
		db:   db,
	}, nil
}

// WC is a Working Copy
// A WC is a directory in the local filesystem, which has a .got/wc.db file
// WC are associated with a single repository each.
type WC struct {
	root             *os.Root
	repo             *gotrepo.Repo
	closeRepoOnClose bool
	id               gotrepo.WorkingCopyID

	// TODO: eventually we should move away from this interface, but
	// the existing importers and exporters use it.
	fsys posixfs.FS
	db   *sqlutil.Pool
}

func (wc *WC) Repo() *gotrepo.Repo {
	return wc.repo
}

func (wc *WC) Dir() string {
	return wc.root.Name()
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

func (wc *WC) GetHead() (string, error) {
	cfg, err := LoadConfig(wc.root)
	if err != nil {
		return "", err
	}
	return cfg.Head, nil
}

// SetHead sets HEAD to name
// SetHead will check if the staging area is not empty, and if it's not
// the it will check that the desired branch has the same content parameters
// as the current branch.
// If one branch is a fork of another, or they have a common ancestor somewhere,
// the it is very likely that they have the same content parameters.
func (wc *WC) SetHead(ctx context.Context, name string) error {
	desiredBranch, err := wc.repo.GetMark(ctx, gotrepo.FQM{Name: name})
	if err != nil {
		return err
	}
	if err := sqlutil.DoTx(ctx, wc.db, func(conn *sqlutil.Conn) error {
		activeName, err := wc.GetHead()
		if err != nil {
			return err
		}
		activeBranch, err := wc.repo.GetMark(ctx, gotrepo.FQM{Name: activeName})
		if err != nil {
			return err
		}
		// if active branch has the same salt as the desired branch, then
		// there is no check to do.
		// If they have different parameters, then we need to check if the staging area is empty.
		if desiredBranch.Info.Config.Hash() != activeBranch.Info.Config.Hash() {
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
		return nil
	}); err != nil {
		return err
	}

	return wc.setHead(name)
}

func (wc *WC) setHead(branchName string) error {
	return EditConfig(wc.root, func(x Config) Config {
		x.Head = branchName
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

// Scan iterates through all of the files, and indexes them in the database.
func (wc *WC) Scan(ctx context.Context) error {
	conn, err := wc.db.Take(ctx)
	if err != nil {
		return err
	}
	defer wc.db.Put(conn)
	paramHash, err := wc.getParamHash(ctx)
	if err != nil {
		return err
	}
	db := porting.NewDB(conn, *paramHash)
	fsys, err := wc.getFilteredFS(ctx)
	if err != nil {
		return err
	}
	return posixfs.WalkLeaves(ctx, fsys, "", func(dir string, de posixfs.DirEnt) error {
		p := path.Join(dir, de.Name)
		finfo, err := fsys.Stat(p)
		if err != nil {
			return err
		}
		return db.PutInfo(ctx, p, porting.FileInfo{
			ModifiedAt: tai64.FromGoTime(finfo.ModTime()),
			Mode:       de.Mode,
		})
	})
}

// Export overwrites data in the filesystem with data from the Snapshot at HEAD.
// Only tracked paths are overwritten.
func (wc *WC) Export(ctx context.Context) error {
	if emptyStage, err := wc.StageIsEmpty(ctx); err != nil {
		return err
	} else if !emptyStage {
		// we may be able to remove this
		return fmt.Errorf("cannot export to filesystem, staging area must be empty (it's not)")
	}
	mname, err := wc.GetHead()
	if err != nil {
		return nil
	}
	mark, err := wc.repo.GetMark(ctx, gotrepo.FQM{Name: mname})
	if err != nil {
		return nil
	}
	paramHash := mark.Info.Config.Hash()
	spans, err := wc.ListSpans(ctx)
	if err != nil {
		return nil
	}
	conn, err := wc.db.Take(ctx)
	if err != nil {
		return nil
	}
	defer wc.db.Put(conn)
	fsys, err := wc.getFilteredFS(ctx)
	if err != nil {
		return err
	}
	return mark.ViewFS(ctx, func(fsmach *gotfs.Machine, s stores.Reading, root gotfs.Root) error {
		portDB := porting.NewDB(conn, paramHash)
		exp := porting.NewExporter(fsmach, portDB, fsys, wc.moveToTrash)
		for _, span := range spans {
			if err := exp.ExportSpan(ctx, s, s, root, span); err != nil {
				return err
			}
		}
		return nil
	})
}

func (wc *WC) Clobber(ctx context.Context, p string) error {
	mname, err := wc.GetHead()
	if err != nil {
		return nil
	}
	mark, err := wc.repo.GetMark(ctx, gotrepo.FQM{Name: mname})
	if err != nil {
		return nil
	}
	paramHash := mark.Info.Config.Hash()
	conn, err := wc.db.Take(ctx)
	if err != nil {
		return nil
	}
	defer wc.db.Put(conn)
	fsys, err := wc.getFilteredFS(ctx)
	if err != nil {
		return err
	}
	return mark.ViewFS(ctx, func(fsmach *gotfs.Machine, s stores.Reading, root gotfs.Root) error {
		portDB := porting.NewDB(conn, paramHash)
		exp := porting.NewExporter(fsmach, portDB, fsys, wc.moveToTrash)
		return exp.Clobber(ctx, s, s, root, p)
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
	head, err := wc.GetHead()
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
	if wc.closeRepoOnClose {
		if err := wc.repo.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Cleanup removes unreferenced data from the stage.
func (wc *WC) Cleanup(ctx context.Context) error {
	if err := sqlutil.DoTx(ctx, wc.db, func(conn *sqlutil.Conn) error {
		logctx.Infof(ctx, "removing blobs from staging areas")
		if err := wc.cleanupStagingBlobs(ctx); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if err := sqlutil.Borrow(ctx, wc.db, func(conn *sqlutil.Conn) error {
		logctx.Infof(ctx, "truncating WAL...")
		if err := sqlutil.WALCheckpoint(conn); err != nil {
			return err
		}
		logctx.Infof(ctx, "running VACUUM...")
		if err := sqlutil.Vacuum(conn); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (wc *WC) filter(spans []Span) func(p string) bool {
	return func(x string) bool {
		if strings.HasPrefix(x, ".got") {
			return false
		}
		return spansContain(spans, x)
	}
}

func (wc *WC) getFilteredFS(ctx context.Context) (posixfs.FS, error) {
	spans, err := wc.ListSpans(ctx)
	if err != nil {
		return nil, err
	}
	return posixfs.NewFiltered(wc.fsys, wc.filter(spans)), nil
}

// moveToTrash moves a file at path to the trash.
// TODO
func (wc *WC) moveToTrash(p string) error {
	return nil
}

func (wc *WC) getParamHash(ctx context.Context) (*[32]byte, error) {
	name, err := wc.GetHead()
	if err != nil {
		return nil, err
	}
	m, err := wc.repo.GetMark(ctx, gotrepo.FQM{Name: name})
	if err != nil {
		return nil, err
	}
	ph := m.Info.Config.Hash()
	return &ph, nil
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
