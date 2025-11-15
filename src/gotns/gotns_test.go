package gotns

import (
	"testing"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/basicns"
	"github.com/cloudflare/circl/kem"
	"github.com/cloudflare/circl/sign"
	"github.com/stretchr/testify/require"

	"github.com/gotvc/got/src/gotns/internal/gotnsop"
	"github.com/gotvc/got/src/internal/testutil"
)

func TestInit(t *testing.T) {
	ctx := testutil.Context(t)
	bc := bclocal.NewTestService(t)
	nsc := basicns.Client{Service: bc}
	volh, err := nsc.CreateAt(ctx, blobcache.Handle{}, "test", BranchVolumeSpec())
	require.NoError(t, err)

	signPub, sigPriv := newTestSigner(t)
	kemPub, kemPriv := newTestKEM(t)
	gnsc := Client{Blobcache: bc, Machine: New(), ActAs: LeafPrivate{SigPrivateKey: sigPriv, KEMPrivateKey: kemPriv}}
	adminLeaf := gotnsop.NewLeaf(signPub, kemPub)
	admins := []IdentityLeaf{adminLeaf}
	err = gnsc.EnsureInit(ctx, *volh, admins)
	require.NoError(t, err)

	adminGrp, err := gnsc.GetGroup(ctx, *volh, "admin")
	require.NoError(t, err)
	require.Equal(t, "admin", adminGrp.Name)
	require.Equal(t, adminLeaf.ID, adminGrp.Owners[0])
}

func TestCreateBranch(t *testing.T) {
	ctx := testutil.Context(t)
	bc := newTestService(t)
	sigPub, sigPriv := newTestSigner(t)
	kemPub, kemPriv := newTestKEM(t)
	nsh := blobcache.Handle{}
	priv := LeafPrivate{SigPrivateKey: sigPriv, KEMPrivateKey: kemPriv}
	gnsc := Client{Blobcache: bc, Machine: New(), ActAs: priv}

	require.NoError(t, gnsc.EnsureInit(ctx, nsh, []IdentityLeaf{gotnsop.NewLeaf(sigPub, kemPub)}))
	err := gnsc.CreateAlias(ctx, nsh, "test", nil)
	require.NoError(t, err)
	vol, err := gnsc.OpenAt(ctx, nsh, "test", priv)
	require.NoError(t, err)
	t.Log(vol)
}

func newTestService(t *testing.T) *bclocal.Service {
	env := bclocal.NewTestEnv(t)
	env.Schemas["gotns"] = SchemaConstructor
	env.Root = blobcache.DefaultLocalSpec()
	env.Root.Local.HashAlgo = blobcache.HashAlgo_BLAKE2b_256
	env.Root.Local.Schema = blobcache.SchemaSpec{Name: "gotns"}

	return bclocal.NewTestServiceFromEnv(t, env)
}

func newTestSigner(t *testing.T) (sign.PublicKey, sign.PrivateKey) {
	return testutil.NewSigner(t, 0)
}

func newTestKEM(t *testing.T) (kem.PublicKey, kem.PrivateKey) {
	return testutil.NewKEM(t, 0)
}
