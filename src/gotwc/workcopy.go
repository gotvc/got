// Package gotwc implements working copies of Got repositories
package gotwc

import (
	"context"
	"fmt"
	"io/fs"
	"iter"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/gotvc/got/src/gotkv/kvstreams"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/gotwc/internal/dbmig"
	"github.com/gotvc/got/src/gotwc/internal/migrations"
	"github.com/gotvc/got/src/gotwc/internal/sqlutil"
	"github.com/gotvc/got/src/gotwc/internal/staging"
	"go.brendoncarroll.net/exp/slices2"
	"go.brendoncarroll.net/state/posixfs"
	"go.brendoncarroll.net/stdctx/logctx"
)

const (
	configPath = ".got/wc-config"
	dbPath     = ".got/wc.db"

	defaultFileMode = 0o644
	nameMaster      = "master"
)

// Init initializes a new working copy in wcdir
// The working copy will be associated with the given repo.
// cfg.RepoDir will be overriden with repo.Dir().
func Init(repo *gotrepo.Repo, wcRoot *os.Root, cfg Config) error {
	if err := wcRoot.MkdirAll(".got", 0o755); err != nil {
		return err
	}
	cfg.RepoDir = repo.Dir()
	return SaveConfig(wcRoot, cfg)
}

// Open opens a directory as a WorkingCopy
// `wcdir` is a directory containing a .got/wc-config file
//
// TODO: maybe this should take a Repo? and Repo should just manage setting up blobcache
// and creating and deleting stages.
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
	return &WC{
		root: root,
		repo: repo,
		fsys: posixfs.NewDirFS(p),

		db: db,
	}, nil
}

// WC is a Working Copy
// A WC is a directory in the local filesystem, which has a .got/wc.db file
// WC are associated with a single repository each.
type WC struct {
	root             *os.Root
	repo             *gotrepo.Repo
	closeRepoOnClose bool

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
		// If they have different salts, then we need to check if the staging area is empty.
		if desiredBranch.Info.Salt != activeBranch.Info.Salt {
			sa, err := newStagingArea(conn, &activeBranch.Info)
			if err != nil {
				return err
			}
			stage := staging.New(sa)
			isEmpty, err := stage.IsEmpty(ctx)
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
		if err := wc.cleanupStagingBlobs(ctx, conn); err != nil {
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

// AllTracked iterates over all the files that are currently tracked.
func (wc *WC) AllTracked(ctx context.Context) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		if err := func() error {
			spans, err := wc.ListSpans(ctx)
			if err != nil {
				return err
			}
			return fs.WalkDir(wc.root.FS(), "", func(p string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				p2 := path.Join(p, d.Name())
				if spansContain(spans, p2) {
					if !yield(p2, nil) {
						return nil
					}
				}
				return nil
			})
		}(); err != nil {
			yield("", err)
		}
	}
}

func (wc *WC) getFilteredFS(spans []Span) posixfs.FS {
	return posixfs.NewFiltered(wc.fsys, func(x string) bool {
		if strings.HasPrefix(x, ".got") {
			return false
		}
		return spansContain(spans, x)
	})
}

// TODO: this is to be more similar to the repo, remove this.
func (wc *WC) workingDir() posixfs.FS {
	return wc.getFilteredFS([]Span{{}})
}

// Span is a span of paths
type Span struct {
	Begin string
	End   string
}

func (s Span) IsPrefix() bool {
	return s.End == string(kvstreams.PrefixEnd([]byte(s.Begin)))
}

func (s Span) String() string {
	if s.End == "" {
		return fmt.Sprintf("<= %q", s.Begin)
	}
	return fmt.Sprintf("[%q %q)", s.Begin, s.End)
}

func PrefixSpan(prefix string) Span {
	return Span{
		Begin: prefix,
		End:   string(kvstreams.PrefixEnd([]byte(prefix))),
	}
}

func (s Span) Contains(x string) bool {
	if x < s.Begin {
		return false
	}
	if s.End != "" && x >= s.End {
		return false
	}
	return true
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
