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
	"github.com/gotvc/got/src/gotwc"
	"github.com/gotvc/got/src/internal/testutil"
)

type Site = gottests.Site

var (
	localMaster  = gotrepo.FQM{Name: "master"}
	originMaster = gotrepo.FQM{Space: "origin", Name: "master"}
)

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
	origin.ConfigureRepo(func(cfg gotrepo.Config) gotrepo.Config {
		// add the PeerIDs to the origin policy
		list := cfg.Blobcache.InProcess.CanTouch
		for _, s := range sites[1:] {
			list = append(list, s.Repo.BlobcachePeer())
		}
		cfg.Blobcache.InProcess.CanTouch = list
		return cfg
	})
	go origin.Repo.Serve(ctx, testutil.PacketConn(t))
	originURL, err := origin.Repo.NSVolumeURL(ctx)
	require.NoError(t, err)

	// configure other repos to use it.
	for _, s := range sites[1:] {
		err := s.Repo.Configure(func(x gotrepo.Config) gotrepo.Config {
			x.Spaces["origin"] = gotrepo.SpaceSpec{Blobcache: originURL}
			return x
		})
		require.NoError(t, err)
		go s.Repo.Serve(ctx, testutil.PacketConn(t))
	}

	// use sites[1] to create marks on origin.
	sites[1].CreateMark(originMaster)
	sites[1].CreateMark(gotrepo.FQM{Space: "origin", Name: "mybranch"})
	require.Contains(t, sites[1].ListMarks("origin"), "master")
	require.Contains(t, sites[1].ListMarks("origin"), "mybranch")

	// create local master branch on sites[1] add some content, and sync.
	testData := []byte("hello world\n")
	sites[1].CreateMark(localMaster)
	sites[1].CreateFile("myfile.txt", testData)
	sites[1].Add("myfile.txt")
	sites[1].Commit(gotwc.CommitParams{})
	sites[1].Sync(localMaster, originMaster)

	// create local master on sites[2] and sync content into it.
	sites[2].CreateMark(localMaster)
	sites[2].Sync(originMaster, localMaster)
	require.Contains(t, sites[2].Ls(localMaster, ""), "myfile.txt")
	require.Equal(t, testData, sites[2].Cat(localMaster, "myfile.txt"))
}

func TestClone(t *testing.T) {
	ctx := testutil.Context(t)
	origin := gottests.NewSite(t)
	go origin.Repo.Serve(ctx, testutil.PacketConn(t))
	origin.CreateMark(localMaster)
	origin.CreateFile("test.txt", []byte("hello world\n"))
	origin.Add("test.txt")
	origin.Commit(gotwc.CommitParams{})

	s1 := origin.Clone()
	s2 := origin.Clone()
	go s1.Repo.Serve(ctx, testutil.PacketConn(t))
	go s2.Repo.Serve(ctx, testutil.PacketConn(t))

	s1.Fetch()
	s2.Fetch()

	require.Contains(t, s1.ListMarks(""), "remote/origin/master")
	require.Contains(t, s2.ListMarks(""), "remote/origin/master")
}

func TestOrg(t *testing.T) {
	ctx := testutil.Context(t)
	ctx, cf := context.WithCancel(ctx)
	t.Cleanup(cf)

	var sites [2]gottests.Site
	for i := range sites {
		sites[i] = gottests.NewSite(t)
	}
	// Setup a standalone blobcache (not part of Got)
	originBC := bclocal.NewTestService(t)
	originEP, err := originBC.Endpoint(ctx)
	require.NoError(t, err)
	orgVolh := blobcachetests.CreateVolume(t, originBC, nil, gotorg.DefaultVolumeSpec(false))
	originURL := &blobcache.URL{
		Node:   originEP.Peer,
		IPPort: &originEP.IPPort,
		Base:   orgVolh.OID,
	}

	gnsc := sites[0].OrgClient()
	gnsc.Blobcache = originBC
	require.NoError(t, gnsc.EnsureInit(ctx, orgVolh, []gotorg.IdentityUnit{
		sites[0].GetIdentity(gotrepo.DefaultIden),
		sites[1].GetIdentity(gotrepo.DefaultIden),
	}))

	// configure other repos to use it.
	for _, s := range sites[1:] {
		err := s.Repo.Configure(func(x gotrepo.Config) gotrepo.Config {
			x.Spaces = map[string]gotrepo.SpaceSpec{
				"origin": {Org: originURL},
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

func getOne[K comparable, V any](m map[K]V) K {
	for k := range m {
		return k
	}
	panic("no keys in map")
}
