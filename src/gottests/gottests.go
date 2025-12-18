package gottests

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotorg"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/gotwc"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/gotvc/got/src/marks"
	"github.com/stretchr/testify/require"
)

// Site is a gotrepo and working copy
type Site struct {
	Path string
	Root *os.Root
	Repo *gotrepo.Repo
	WC   *gotwc.WC

	t testing.TB
}

func NewSite(t testing.TB) Site {
	dirpath := t.TempDir()
	repoCfg := gotrepo.DefaultConfig()
	require.NoError(t, gotrepo.Init(dirpath, repoCfg))

	root, err := os.OpenRoot(dirpath)
	require.NoError(t, err)
	t.Cleanup(func() {
		root.Close()
	})
	repo, err := gotrepo.Open(dirpath)
	require.NoError(t, err)
	t.Cleanup(func() {
		repo.Close()
	})
	wcCfg := gotwc.Config{
		Head:  "master",
		ActAs: gotrepo.DefaultIden,
	}
	require.NoError(t, gotwc.Init(repo, dirpath, wcCfg))
	wc, err := gotwc.New(repo, root)
	require.NoError(t, err)
	return Site{
		Path: dirpath,
		Root: root,
		Repo: repo,
		WC:   wc,

		t: t,
	}
}

func (s *Site) CreateFile(p string, data []byte) {
	require.NoError(s.t, s.Root.WriteFile(p, data, 0o644))
}

func (s *Site) CreateMark(fqname gotrepo.FQM) {
	ctx := testutil.Context(s.t)
	_, err := s.Repo.CreateMark(ctx, fqname, marks.Params{})
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

func (s *Site) Commit() {
	ctx := testutil.Context(s.t)
	err := s.WC.Commit(ctx, marks.SnapInfo{})
	require.NoError(s.t, err)
}

func (s *Site) Sync(src, dst gotrepo.FQM) {
	ctx := testutil.Context(s.t)
	err := s.Repo.Sync(ctx, src, dst, false)
	require.NoError(s.t, err)
}

func (s *Site) Add(p string) {
	err := s.WC.Add(testutil.Context(s.t), p)
	require.NoError(s.t, err)
}

func (s *Site) Ls(b gotrepo.FQM, p string) (ret []string) {
	err := s.Repo.Ls(testutil.Context(s.t), b, p, func(de gotfs.DirEnt) error {
		ret = append(ret, de.Name)
		return nil
	})
	require.NoError(s.t, err)
	return ret
}

func (s *Site) Cat(b gotrepo.FQM, p string) []byte {
	buf := bytes.Buffer{}
	err := s.Repo.Cat(testutil.Context(s.t), b, p, &buf)
	require.NoError(s.t, err)
	return buf.Bytes()
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
