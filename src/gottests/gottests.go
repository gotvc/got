package gottests

import (
	"bytes"
	"context"
	"io/fs"
	"iter"
	"net"
	"os"
	"path"
	"regexp"
	"strings"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotorg"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/gotwc"
	"github.com/gotvc/got/src/internal/gotbc"
	"github.com/gotvc/got/src/internal/gotcore"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

// Site is a gotrepo, working copy, and in-process Blobcache
type Site struct {
	// Root is the directory containing all of the data for this Site
	Root *os.Root
	// WC is the working copy, using Root
	WC *gotwc.WC
	// Blobcache is an in-process Blobcache
	Blobcache blobcache.Service
	Repo      *gotrepo.Repo

	t testing.TB
}

// NewSite creates a new tempdir in the test, calls gotwc.Init with the default config
// then opens the WC and fills the remaining fields in the site
func NewSite(t testing.TB) Site {
	return newSite(t, gotrepo.DefaultConfig())
}

func newSite(t testing.TB, repoConfig gotrepo.Config) Site {
	ctx := t.Context()
	dirpath := t.TempDir()
	root := testutil.OpenRoot(t, dirpath)
	t.Cleanup(func() {
		require.NoError(t, root.Close())
	})
	wcCfg := gotwc.DefaultConfig()

	// setup repo
	func() {
		bc, err := gotbc.OpenBlobcache(root, wcCfg.Blobcache, t.Context())
		require.NoError(t, err)
		defer func() { require.NoError(t, bc.(*gotbc.Local).Close()) }()
		volh, err := bc.OpenFiat(ctx, wcCfg.Repo, blobcache.Action_ALL)
		require.NoError(t, err)
		require.NoError(t, gotrepo.Init(t.Context(), bc, *volh, repoConfig))
	}()

	require.NoError(t, gotwc.Init(root, wcCfg))
	return openSite(t, root)
}

// openSite opens the working copy and uses it to fill out the Site
func openSite(t testing.TB, root *os.Root) Site {
	// wc
	wc, err := gotwc.Open(root)
	require.NoError(t, err)
	t.Cleanup(func() {
		wc.Close()
	})

	repo := wc.Repo()
	return Site{
		Root:      root,
		WC:        wc,
		Blobcache: repo.Blobcache(),
		Repo:      repo,

		t: t,
	}
}

func (s *Site) ConfigureRepo(ctx context.Context, fn func(gotrepo.Config) gotrepo.Config) {
	require.NoError(s.t, s.Repo.Configure(ctx, fn))
}

func (s *Site) ConfigureWC(ctx context.Context, fn func(gotwc.Config) gotwc.Config) {
	require.NoError(s.t, s.WC.Configure(ctx, fn))
}

func (s *Site) BlobcacheNodeID() blobcache.NodeID {
	return s.Blobcache.(*gotbc.Local).LocalNode()
}

// Clone creates a new site, by cloning the this site.
func (s *Site) Clone() Site {
	ctx := testutil.Context(s.t)
	vspec, err := s.Repo.NSVolumeSpec(ctx)
	require.NoError(s.t, err)

	// create and open other site
	other := NewSite(s.t)
	other.ConfigureRepo(ctx, func(c gotrepo.Config) gotrepo.Config {
		return *c.PutSpace("origin", gotrepo.SpaceSpec{Blobcache: vspec}).
			AddPull(gotrepo.PullConfig{
				From:      "origin",
				Filter:    regexp.MustCompile(".*"),
				AddPrefix: "remote/origin/",
			})
	})
	s.ConfigureWC(ctx, ConfigAddTouch(other.BlobcacheNodeID()))
	return other
}

func (s *Site) CheckAll() {
	ctx := testutil.Context(s.t)
	require.NoError(s.t, s.Repo.CheckAll(ctx))
}

func (s *Site) Pull() {
	ctx := testutil.Context(s.t)
	require.NoError(s.t, s.Repo.Pull(ctx, nil))
}

func (s *Site) Push() {
	ctx := testutil.Context(s.t)
	require.NoError(s.t, s.Repo.Push(ctx, nil))
}

func (s *Site) CreateFile(p string, data []byte) {
	if dir := path.Dir(p); dir != "." {
		require.NoError(s.t, s.Root.MkdirAll(dir, 0o755))
	}
	require.NoError(s.t, s.Root.WriteFile(p, data, 0o644))
}

func (s *Site) CreateMark(fqname gotrepo.FQM) {
	ctx := testutil.Context(s.t)
	_, err := s.Repo.CreateMark(ctx, fqname, gotcore.DefaultConfig(true), nil)
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

func (s *Site) Ls(se gotcore.CommitExpr, p string) (ret []string) {
	err := s.Repo.Ls(testutil.Context(s.t), se, p, func(de gotfs.DirEnt) error {
		ret = append(ret, de.Name)
		return nil
	})
	require.NoError(s.t, err)
	return ret
}

func (s *Site) Cat(se gotcore.CommitExpr, p string) []byte {
	buf := bytes.Buffer{}
	err := s.Repo.Cat(testutil.Context(s.t), se, p, &buf)
	require.NoError(s.t, err)
	return buf.Bytes()
}

func (s *Site) GetHead() string {
	head, err := s.WC.GetSaveTo()
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

func (s *Site) Serve(ctx context.Context, pc net.PacketConn) error {
	return s.Blobcache.(*gotbc.Local).Serve(ctx, pc)
}

func ConfigAddTouch(peer blobcache.NodeID) func(cfg gotwc.Config) gotwc.Config {
	return func(cfg gotwc.Config) gotwc.Config {
		allowed := cfg.Blobcache.InProcess.CanTouch
		allowed = append(allowed, peer)
		cfg.Blobcache.InProcess.CanTouch = allowed
		return cfg
	}
}
