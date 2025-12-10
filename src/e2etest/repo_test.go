package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/stretchr/testify/require"

	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotorg"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/internal/testutil"
)

func TestMultiRepoSync(t *testing.T) {
	ctx := testutil.Context(t)
	ctx, cf := context.WithCancel(ctx)
	t.Cleanup(cf)
	p1, p2, pOrigin := initRepo(t), initRepo(t), initRepo(t)
	origin := openRepo(t, pOrigin)
	go origin.Serve(ctx, testutil.PacketConn(t))
	originNS, err := origin.GotNSVolume(ctx)
	require.NoError(t, err)
	for _, p := range []string{p1, p2} {
		err := gotrepo.ConfigureRepo(p, func(x gotrepo.Config) gotrepo.Config {
			originEP := origin.Endpoint()

			x.Spaces = []gotrepo.SpaceLayerSpec{
				{
					Prefix: "origin/",
					Target: gotrepo.SpaceSpec{Blobcache: &gotrepo.VolumeSpec{
						Remote: &blobcache.VolumeBackend_Remote{
							Endpoint: originEP,
							Volume:   originNS.OID,
						},
					}},
				},
			}
			return x
		})
		require.NoError(t, err)
	}
	r1, r2 := openRepo(t, p1), openRepo(t, p2)

	// IAM setup
	gnsc := origin.GotNSClient()
	intro1, err := r1.IntroduceSelf(ctx)
	require.NoError(t, err)
	intro2, err := r2.IntroduceSelf(ctx)
	require.NoError(t, err)
	originLeaf, err := origin.ActiveIdentity(ctx)
	require.NoError(t, err)
	require.NoError(t, gnsc.EnsureInit(ctx, blobcache.Handle{OID: originNS.OID}, []gotorg.IdentityUnit{originLeaf}))
	// Handles with empty secrets cause OpenAs to be called instead of OpenFrom.
	require.NoError(t, gnsc.Do(ctx, blobcache.Handle{OID: originNS.OID}, func(tx *gotorg.Txn) error {
		for _, intro := range []gotorg.ChangeSet{intro1, intro2} {
			if err := tx.ChangeSet(ctx, intro); err != nil {
				return err
			}
			g, err := tx.LookupGroup(ctx, "admin")
			require.NoError(t, err)
			id := getOne(intro.Sigs)
			if err := tx.AddMember(ctx, g.ID, gotorg.MemberUnit(id)); err != nil {
				return err
			}
		}
		return nil
	}))

	t.SkipNow() // TODO: need to be able to create blobcache volumes on the remote.
	createBranch(t, r1, "origin/master")
	createBranch(t, r1, "origin/mybranch")
	require.Contains(t, listBranches(t, r2), "origin/master")
	require.Contains(t, listBranches(t, r2), "origin/mybranch")

	testData := []byte("hello world\n")
	createFile(t, p1, "myfile.txt", testData)
	add(t, r1, "myfile.txt")
	commit(t, r1)
	sync(t, r1, "master", "origin/master")

	sync(t, r2, "origin/master", "master")
	require.Contains(t, ls(t, r2, ""), "myfile.txt")
	require.Equal(t, testData, cat(t, r2, "myfile.txt"))
}

func initRepo(t testing.TB) string {
	dirpath := t.TempDir()
	cfg := gotrepo.DefaultConfig()
	require.NoError(t, gotrepo.Init(dirpath, cfg))
	return dirpath
}

func openRepo(t testing.TB, p string) *gotrepo.Repo {
	r, err := gotrepo.Open(p)
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })
	return r
}

func createFile(t testing.TB, repoPath, p string, data []byte) {
	err := os.WriteFile(filepath.Join(repoPath, p), data, 0o644)
	require.NoError(t, err)
}

func createBranch(t testing.TB, r *gotrepo.Repo, name string) {
	ctx := testutil.Context(t)
	_, err := r.CreateBranch(ctx, name, branches.Params{})
	require.NoError(t, err)
}

func commit(t testing.TB, r *gotrepo.Repo) {
	ctx := testutil.Context(t)
	err := r.Commit(ctx, branches.SnapInfo{})
	require.NoError(t, err)
}

func sync(t testing.TB, r *gotrepo.Repo, src, dst string) {
	ctx := testutil.Context(t)
	err := r.Sync(ctx, src, dst, false)
	require.NoError(t, err)
}

func add(t testing.TB, r *gotrepo.Repo, p string) {
	err := r.Add(testutil.Context(t), p)
	require.NoError(t, err)
}

func ls(t testing.TB, r *gotrepo.Repo, p string) (ret []string) {
	err := r.Ls(testutil.Context(t), p, func(de gotfs.DirEnt) error {
		ret = append(ret, de.Name)
		return nil
	})
	require.NoError(t, err)
	return ret
}

func cat(t testing.TB, r *gotrepo.Repo, p string) []byte {
	buf := bytes.Buffer{}
	err := r.Cat(context.TODO(), p, &buf)
	require.NoError(t, err)
	return buf.Bytes()
}

func listBranches(t testing.TB, r *gotrepo.Repo) (ret []string) {
	err := r.ForEachBranch(context.TODO(), func(name string) error {
		ret = append(ret, name)
		return nil
	})
	require.NoError(t, err)
	return ret
}

func getOne[K comparable, V any](m map[K]V) K {
	for k := range m {
		return k
	}
	panic("no keys in map")
}
