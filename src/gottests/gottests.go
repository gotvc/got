package gottests

import (
	"bytes"
	"context"
	"io/fs"
	"iter"
	"os"
	"regexp"
	"strings"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotorg"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/gotwc"
	"github.com/gotvc/got/src/internal/marks"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

// Site is a gotrepo and working copy
type Site struct {
	Root *os.Root
	Repo *gotrepo.Repo
	WC   *gotwc.WC

	t testing.TB
}

func NewSite(t testing.TB) Site {
	dirpath := t.TempDir()
	repoCfg := gotrepo.DefaultConfig()
	repoRoot := testutil.OpenRoot(t, dirpath)
	t.Cleanup(func() {
		require.NoError(t, repoRoot.Close())
	})
	require.NoError(t, gotrepo.Init(repoRoot, repoCfg))
	return openSite(t, repoRoot)
}

func openSite(t testing.TB, root *os.Root) Site {
	repo, err := gotrepo.Open(root)
	require.NoError(t, err)
	t.Cleanup(func() {
		repo.Close()
	})
	require.NoError(t, gotwc.Init(repo, root, gotwc.DefaultConfig()))
	wc, err := gotwc.New(repo, root)
	require.NoError(t, err)
	return Site{
		Root: root,
		Repo: repo,
		WC:   wc,

		t: t,
	}
}

func (s *Site) ConfigureRepo(fn func(gotrepo.Config) gotrepo.Config) {
	require.NoError(s.t, s.Repo.Configure(fn))
}

// Clone creates a new site, by cloning the this site.
func (s *Site) Clone() Site {
	ctx := testutil.Context(s.t)
	dirpath := s.t.TempDir()
	repoRoot := testutil.OpenRoot(s.t, dirpath)

	repoCfg := gotrepo.DefaultConfig()
	vspec, err := s.Repo.NSVolumeSpec(ctx)
	require.NoError(s.t, err)
	repoCfg.PutSpace("origin", gotrepo.SpaceSpec{Blobcache: vspec})
	repoCfg.AddFetch(gotrepo.FetchConfig{
		From:      "origin",
		Filter:    regexp.MustCompile(".*"),
		AddPrefix: "remote/origin/",
	})
	require.NoError(s.t, gotrepo.Init(repoRoot, repoCfg))
	other := openSite(s.t, repoRoot)
	s.ConfigureRepo(ConfigAddTouch(other.Repo.BlobcachePeer()))
	return other
}

func (s *Site) Fetch() {
	ctx := testutil.Context(s.t)
	require.NoError(s.t, s.Repo.Fetch(ctx))
}

func (s *Site) CreateFile(p string, data []byte) {
	require.NoError(s.t, s.Root.WriteFile(p, data, 0o644))
}

func (s *Site) CreateMark(fqname gotrepo.FQM) {
	ctx := testutil.Context(s.t)
	_, err := s.Repo.CreateMark(ctx, fqname, marks.DefaultConfig(true), nil)
	require.NoError(s.t, err)
}

func (s *Site) ListMarks(space string) (ret []string) {
	err := s.Repo.ForEachMark(context.TODO(), space, func(name string) error {
		ret = append(ret, name)
		return nil
	})
	require.NoError(s.t, err)
	return ret
}

func (s *Site) Commit(cp gotwc.CommitParams) {
	ctx := testutil.Context(s.t)
	err := s.WC.Commit(ctx, cp)
	require.NoError(s.t, err)
}

func (s *Site) Sync(src, dst gotrepo.FQM) {
	ctx := testutil.Context(s.t)
	err := s.Repo.SyncUnit(ctx, src, dst, false)
	require.NoError(s.t, err)
}

func (s *Site) Fork(newName string) {
	ctx := testutil.Context(s.t)
	err := s.WC.Fork(ctx, newName)
	require.NoError(s.t, err)
}

func (s *Site) Checkout(name string) {
	ctx := testutil.Context(s.t)
	err := s.WC.Checkout(ctx, name)
	require.NoError(s.t, err)
}

func (s *Site) Add(ps ...string) {
	ctx := testutil.Context(s.t)
	for _, p := range ps {
		err := s.WC.Add(ctx, p)
		require.NoError(s.t, err)
	}
}

func (s *Site) Put(ps ...string) {
	ctx := testutil.Context(s.t)
	for _, p := range ps {
		err := s.WC.Put(ctx, p)
		require.NoError(s.t, err)
	}
}

func (s *Site) Ls(se marks.SnapExpr, p string) (ret []string) {
	err := s.Repo.Ls(testutil.Context(s.t), se, p, func(de gotfs.DirEnt) error {
		ret = append(ret, de.Name)
		return nil
	})
	require.NoError(s.t, err)
	return ret
}

func (s *Site) Cat(se marks.SnapExpr, p string) []byte {
	buf := bytes.Buffer{}
	err := s.Repo.Cat(testutil.Context(s.t), se, p, &buf)
	require.NoError(s.t, err)
	return buf.Bytes()
}

func (s *Site) GetHead() string {
	head, err := s.WC.GetHead()
	require.NoError(s.t, err)
	return head
}

func (s *Site) SetHead(name string) {
	ctx := testutil.Context(s.t)
	require.NoError(s.t, s.WC.SetHead(ctx, name))
}

func (s *Site) GetIdentity(name string) gotorg.IdentityUnit {
	idu, err := s.Repo.GetIdentity(testutil.Context(s.t), name)
	require.NoError(s.t, err)
	return *idu
}

func (s *Site) OrgClient() gotorg.Client {
	c, err := s.Repo.OrgClient(gotrepo.DefaultIden)
	require.NoError(s.t, err)
	return c
}

func (s *Site) IntroduceSelf() gotorg.ChangeSet {
	oc := s.OrgClient()
	return oc.IntroduceSelf()
}

func (s *Site) WriteFSMap(x map[string]string) {
	for k, v := range x {
		s.WriteString(k, v)
	}
}

func (s *Site) WriteString(p string, val string) {
	data, err := s.Root.ReadFile(p)
	if !os.IsNotExist(err) {
		require.NoError(s.t, err)
	}
	if err == nil && string(data) == val {
		return
	}
	require.NoError(s.t, s.Root.WriteFile(p, []byte(val), 0o644))
}

func (s *Site) DeleteFile(ps ...string) {
	for _, p := range ps {
		require.NoError(s.t, s.Root.Remove(p))
	}
}

func (s *Site) AllPaths() iter.Seq[string] {
	return func(yield func(p string) bool) {
		require.NoError(s.t, fs.WalkDir(s.Root.FS(), ".", func(p string, de fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if p == "." {
				return nil
			}
			if strings.HasPrefix(p, ".got") {
				return nil
			}
			if !yield(p) {
				return nil
			}
			return nil
		}))
	}
}

func ConfigAddTouch(peer blobcache.PeerID) func(cfg gotrepo.Config) gotrepo.Config {
	return func(cfg gotrepo.Config) gotrepo.Config {
		allowed := cfg.Blobcache.InProcess.CanTouch
		allowed = append(allowed, peer)
		cfg.Blobcache.InProcess.CanTouch = allowed
		return cfg
	}
}
