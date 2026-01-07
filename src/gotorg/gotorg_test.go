package gotorg

import (
	"fmt"
	"testing"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"blobcache.io/blobcache/src/schema/bcns"
	_ "blobcache.io/blobcache/src/schema/jsonns"
	"github.com/cloudflare/circl/kem"
	"github.com/cloudflare/circl/sign"
	"github.com/stretchr/testify/require"

	"github.com/gotvc/got/src/gotorg/internal/gotorgop"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/testutil"
)

func TestInit(t *testing.T) {
	ctx := testutil.Context(t)
	bc := bclocal.NewTestService(t)
	rootish := bcns.Objectish{}
	nsc, err := bcns.ClientForVolume(ctx, bc, rootish)
	require.NoError(t, err)
	nsh, err := rootish.Open(ctx, bc)
	require.NoError(t, err)

	volh, err := nsc.CreateAt(ctx, *nsh, "test", BranchVolumeSpec())
	require.NoError(t, err)

	signPub, sigPriv := newTestSigner(t)
	kemPub, kemPriv := newTestKEM(t)
	gnsc := Client{Blobcache: bc, Machine: New(), ActAs: IdenPrivate{SigPrivateKey: sigPriv, KEMPrivateKey: kemPriv}}
	adminLeaf := NewIDUnit(signPub, kemPub)
	admins := []IdentityUnit{adminLeaf}
	err = gnsc.EnsureInit(ctx, *volh, admins)
	require.NoError(t, err)

	adminGrp, err := gnsc.LookupGroup(ctx, *volh, "admin")
	require.NoError(t, err)
	require.Equal(t, adminLeaf.ID, adminGrp.Owners[0])
}

func TestCreateBranch(t *testing.T) {
	ctx := testutil.Context(t)
	bc := newTestService(t)
	sigPub, sigPriv := newTestSigner(t)
	kemPub, kemPriv := newTestKEM(t)
	nsh := blobcache.Handle{}
	priv := IdenPrivate{SigPrivateKey: sigPriv, KEMPrivateKey: kemPriv}
	gnsc := Client{Blobcache: bc, Machine: New(), ActAs: priv}

	require.NoError(t, gnsc.EnsureInit(ctx, nsh, []IdentityUnit{gotorgop.NewIDUnit(sigPub, kemPub)}))
	err := gnsc.CreateAlias(ctx, nsh, "test", nil)
	require.NoError(t, err)
	vol, err := gnsc.OpenAt(ctx, nsh, "test", priv, false)
	require.NoError(t, err)
	t.Log(vol)
}

func TestPutGetIDUnit(t *testing.T) {
	ctx := testutil.Context(t)
	sigPub, sigPriv := newTestSigner(t)
	kemPub, kemPriv := newTestKEM(t)
	priv := IdenPrivate{SigPrivateKey: sigPriv, KEMPrivateKey: kemPriv}
	m := New()
	s := stores.NewMem()
	root, err := m.New(ctx, s, []IdentityUnit{NewIDUnit(sigPub, kemPub)})
	require.NoError(t, err)
	idu := NewIDUnit(sigPub, kemPub)
	state, err := m.PutIDUnit(ctx, s, root.State.Current, idu)
	require.NoError(t, err)
	idu2, err := m.GetIDUnit(ctx, s, *state, priv.GetID())
	require.NoError(t, err)
	require.Equal(t, idu2, &idu)
}

func newTestService(t *testing.T) *bclocal.Service {
	env := bclocal.NewTestEnv(t)
	env.MkSchema = func(spec blobcache.SchemaSpec) (schema.Schema, error) {
		switch spec.Name {
		case "gotorg":
			return SchemaConstructor(spec.Params, nil)
		case "":
			return schema.NoneConstructor(spec.Params, nil)
		default:
			return nil, fmt.Errorf("unknown schema %s", spec.Name)
		}
	}
	env.Root = blobcache.DefaultLocalSpec()
	env.Root.Local.HashAlgo = blobcache.HashAlgo_BLAKE2b_256
	env.Root.Local.Schema = blobcache.SchemaSpec{Name: "gotorg"}

	return bclocal.NewTestServiceFromEnv(t, env)
}

func newTestSigner(t *testing.T) (sign.PublicKey, sign.PrivateKey) {
	return testutil.NewSigner(t, 0)
}

func newTestKEM(t *testing.T) (kem.PublicKey, kem.PrivateKey) {
	return testutil.NewKEM(t, 0)
}
