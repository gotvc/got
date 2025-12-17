package main

import (
	"context"
	"testing"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"github.com/stretchr/testify/require"

	"github.com/gotvc/got/src/gotorg"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/gottests"
	"github.com/gotvc/got/src/internal/testutil"
)

type Site = gottests.Site

func TestMultiRepoSync(t *testing.T) {
	ctx := testutil.Context(t)
	ctx, cf := context.WithCancel(ctx)
	t.Cleanup(cf)

	var sites [3]gottests.Site
	for i := range sites {
		sites[i] = gottests.NewSite(t)
	}
	origin := sites[0]

	// serve the origin repo
	go origin.Repo.Serve(ctx, testutil.PacketConn(t))
	originEP := origin.Repo.Endpoint()
	originNS, err := origin.Repo.NSVolume(ctx)
	require.NoError(t, err)

	// configure other repos to use it.
	for _, s := range sites[1:] {
		err := s.Repo.Configure(func(x gotrepo.Config) gotrepo.Config {
			x.Spaces = []gotrepo.SpaceConfig{
				{
					Name: "origin",
					Spec: gotrepo.SpaceSpec{
						Org: &blobcache.VolumeSpec{
							Remote: &blobcache.VolumeBackend_Remote{
								Endpoint: originEP,
								Volume:   originNS.OID,
							},
						},
					},
				},
			}
			return x
		})
		require.NoError(t, err)
	}

	t.SkipNow() // TODO: need to be able to create blobcache volumes on the remote.
	sites[1].CreateMark("origin/master")
	sites[1].CreateMark("origin/mybranch")
	require.Contains(t, sites[1].ListMarks("origin"), "master")
	require.Contains(t, sites[1].ListMarks("origin"), "mybranch")

	testData := []byte("hello world\n")
	sites[1].CreateFile("myfile.txt", testData)
	sites[1].Add("myfile.txt")
	sites[1].Commit()
	sites[1].Sync("master", "origin/master")

	sites[2].Sync("origin/master", "master")
	require.Contains(t, sites[2].Ls("master", ""), "myfile.txt")
	require.Equal(t, testData, sites[2].Cat("master", "myfile.txt"))
}

func getOne[K comparable, V any](m map[K]V) K {
	for k := range m {
		return k
	}
	panic("no keys in map")
}

func TestOrgSync(t *testing.T) {
	ctx := testutil.Context(t)
	ctx, cf := context.WithCancel(ctx)
	t.Cleanup(cf)

	var sites [2]gottests.Site
	for i := range sites {
		sites[i] = gottests.NewSite(t)
	}
	originBC := bclocal.NewTestService(t)
	originEP, err := originBC.Endpoint(ctx)
	require.NoError(t, err)
	orgVolh := blobcachetests.CreateVolume(t, originBC, nil, gotorg.DefaultVolumeSpec(false))
	gnsc := gotorg.Client{
		Blobcache: originBC,
		Machine:   gotorg.New(),
	}
	require.NoError(t, gnsc.EnsureInit(ctx, orgVolh, []gotorg.IdentityUnit{
		sites[0].GetIdentity(gotrepo.DefaultIden),
		sites[1].GetIdentity(gotrepo.DefaultIden),
	}))

	// configure other repos to use it.
	for _, s := range sites[1:] {
		err := s.Repo.Configure(func(x gotrepo.Config) gotrepo.Config {
			x.Spaces = []gotrepo.SpaceConfig{
				{
					Name: "origin",
					Spec: gotrepo.SpaceSpec{
						Org: &blobcache.VolumeSpec{
							Remote: &blobcache.VolumeBackend_Remote{
								Endpoint: originEP,
								Volume:   orgVolh.OID,
							},
						},
					},
				},
			}
			return x
		})
		require.NoError(t, err)

		intro := s.IntroduceSelf()

		// Handles with empty secrets cause OpenAs to be called instead of OpenFrom.
		require.NoError(t, gnsc.Do(ctx, orgVolh, func(tx *gotorg.Txn) error {
			for _, intro := range []gotorg.ChangeSet{intro} {
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
	}
}
