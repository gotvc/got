package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/inet256/inet256/networks/beaconnet"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/inet256/inet256/pkg/inet256d"
	"github.com/inet256/inet256/pkg/mesh256"
	"github.com/stretchr/testify/require"

	"github.com/gotvc/got"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gothost"
	"github.com/gotvc/got/pkg/gotrepo"
	"github.com/gotvc/got/pkg/gotvc"
	"github.com/gotvc/got/pkg/testutil"
)

func TestMultiRepoSync(t *testing.T) {
	ctx := testutil.Context(t)
	ctx, cf := context.WithCancel(ctx)
	t.Cleanup(cf)
	secretKey := [32]byte{}
	p1, p2, pOrigin := initRepo(t), initRepo(t), initRepo(t)
	origin := openRepo(t, pOrigin)
	for _, p := range []string{p1, p2} {
		err := got.ConfigureRepo(p, func(x got.RepoConfig) got.RepoConfig {
			originID := origin.GetID()
			x.Spaces = []gotrepo.SpaceLayerSpec{
				{
					Prefix: "origin/",
					Target: gotrepo.SpaceSpec{
						Crypto: &gotrepo.CryptoSpaceSpec{
							Inner:  gotrepo.SpaceSpec{Peer: &originID},
							Secret: secretKey[:],
						},
					},
				},
			}
			return x
		})
		require.NoError(t, err)
	}
	r1, r2 := openRepo(t, p1), openRepo(t, p2)

	// IAM setup
	e := origin.GetHostEngine()
	err := e.ModifyPolicy(ctx, func(x gothost.Policy) gothost.Policy {
		return gothost.Policy{
			Rules: []gothost.Rule{
				{Allow: true, Subject: gothost.NewPeer(r1.GetID()), Verb: gothost.OpTouch, Object: regexp.MustCompile(".*")},
				{Allow: true, Subject: gothost.NewPeer(r2.GetID()), Verb: gothost.OpTouch, Object: regexp.MustCompile(".*")},
			},
		}
	})
	require.NoError(t, err)
	hostConfig, err := e.View(ctx)
	require.NoError(t, err)
	t.Log("RULES", hostConfig.Policy.Rules)

	go origin.Serve(ctx)
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
	require.NoError(t, got.InitRepo(dirpath))
	return dirpath
}

func openRepo(t testing.TB, p string) *got.Repo {
	r, err := got.OpenRepo(p)
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })
	return r
}

func createFile(t testing.TB, repoPath, p string, data []byte) {
	err := os.WriteFile(filepath.Join(repoPath, p), data, 0o644)
	require.NoError(t, err)
}

func createBranch(t testing.TB, r *got.Repo, name string) {
	ctx := testutil.Context(t)
	_, err := r.CreateBranch(ctx, name, branches.Config{})
	require.NoError(t, err)
}

func commit(t testing.TB, r *got.Repo) {
	ctx := testutil.Context(t)
	err := r.Commit(ctx, gotvc.SnapInfo{})
	require.NoError(t, err)
}

func sync(t testing.TB, r *got.Repo, src, dst string) {
	ctx := testutil.Context(t)
	err := r.Sync(ctx, src, dst, false)
	require.NoError(t, err)
}

func add(t testing.TB, r *got.Repo, p string) {
	err := r.Add(testutil.Context(t), p)
	require.NoError(t, err)
}

func ls(t testing.TB, r *got.Repo, p string) (ret []string) {
	err := r.Ls(testutil.Context(t), p, func(de gotfs.DirEnt) error {
		ret = append(ret, de.Name)
		return nil
	})
	require.NoError(t, err)
	return ret
}

func cat(t testing.TB, r *got.Repo, p string) []byte {
	buf := bytes.Buffer{}
	err := r.Cat(context.TODO(), p, &buf)
	require.NoError(t, err)
	return buf.Bytes()
}

func listBranches(t testing.TB, r *got.Repo) (ret []string) {
	err := r.ForEachBranch(context.TODO(), func(name string) error {
		ret = append(ret, name)
		return nil
	})
	require.NoError(t, err)
	return ret
}

const inet256Endpoint = "tcp://127.0.0.1:12345"

func TestMain(m *testing.M) {
	code := func() int {
		ctx, cf := context.WithCancel(context.Background())
		defer cf()
		privateKey, _ := inet256.PrivateKeyFromBuiltIn(ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize)))
		d := inet256d.New(inet256d.Params{
			APIAddr: inet256Endpoint,
			MainNodeParams: mesh256.Params{
				NewNetwork: beaconnet.Factory,
				PrivateKey: privateKey,
				Peers:      mesh256.NewPeerStore(),
			},
		})
		go d.Run(ctx)
		if err := os.Setenv("INET256_API", inet256Endpoint); err != nil {
			panic(err)
		}
		return m.Run()
	}()
	os.Exit(code)
}
