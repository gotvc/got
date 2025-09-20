package gotns

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/basicns"
	"github.com/cloudflare/circl/kem"
	"github.com/cloudflare/circl/sign"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
	"go.inet256.org/inet256/src/inet256"

	"github.com/gotvc/got/src/internal/testutil"
)

func TestInit(t *testing.T) {
	ctx := testutil.Context(t)
	bc := bclocal.NewTestService(t)
	nsc := basicns.Client{Service: bc}
	volh, err := nsc.CreateAt(ctx, blobcache.Handle{}, "test", blobcache.DefaultLocalSpec())
	require.NoError(t, err)

	signPub, sigPriv := newTestSigner(t)
	kemPub, kemPriv := newTestKEM(t)
	gnsc := Client{Blobcache: bc, Machine: New(), ActAs: LeafPrivate{SigPrivateKey: sigPriv, KEMPrivateKey: kemPriv}}
	adminLeaf := NewLeaf(signPub, kemPub)
	admins := []IdentityLeaf{adminLeaf}
	err = gnsc.Init(ctx, *volh, admins)
	require.NoError(t, err)

	adminGrp, err := gnsc.GetGroup(ctx, *volh, "admin")
	require.NoError(t, err)
	require.Equal(t, "admin", adminGrp.Name)
	require.Equal(t, adminLeaf.ID, adminGrp.Owners[0])
}

func TestMarshalGroup(t *testing.T) {
	kemPub, _ := newTestKEM(t)
	group := Group{Name: "test", KEM: kemPub, Owners: nil, LeafKEMs: map[inet256.ID][]byte{}}
	k := group.Key(nil)
	val := group.Value(nil)
	group2, err := ParseGroup(k, val)
	require.NoError(t, err)
	require.Equal(t, group, *group2)
}

func TestMarshalOp(t *testing.T) {
	pubKey, _ := newTestSigner(t)
	kemPub, _ := newTestKEM(t)
	tc := []Op{
		&Op_CreateGroup{Group: Group{Name: "test", KEM: kemPub, Owners: nil, LeafKEMs: map[inet256.ID][]byte{}}},
		&Op_CreateLeaf{Leaf: NewLeaf(pubKey, kemPub)},

		&Op_AddLeaf{Group: "a", LeafID: inet256.ID{}},
		&Op_RemoveLeaf{Group: "a", ID: inet256.ID{}},
		&Op_AddMember{Group: "a", Member: "b", EncryptedKEM: []byte{}},
		&Op_RemoveMember{Group: "a", Member: "b"},
		&Op_AddRule{Rule: Rule{Subject: "sub", Verb: "verb", ObjectType: ObjectType_GROUP, Names: regexp.MustCompile(".*")}},
		&Op_DropRule{RuleID: CID{}},
		&Op_PutEntry{Entry: Entry{Name: "test", Aux: []byte{}}},
		&Op_DeleteEntry{Name: "test"},
	}

	for i, tc := range tc {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			x := tc
			data := AppendOp(nil, x)
			y, rest, err := ReadOp(data)
			require.NoError(t, err)
			require.Len(t, rest, 0)
			require.Equal(t, x, y)
		})
	}
}

func TestCreateAt(t *testing.T) {
	ctx := testutil.Context(t)
	bc := newTestService(t)
	sigPub, sigPriv := newTestSigner(t)
	kemPub, kemPriv := newTestKEM(t)
	gnsc := Client{Blobcache: bc, Machine: New(), ActAs: LeafPrivate{SigPrivateKey: sigPriv, KEMPrivateKey: kemPriv}}
	require.NoError(t, gnsc.Init(ctx, blobcache.Handle{}, []IdentityLeaf{NewLeaf(sigPub, kemPub)}))

	err := gnsc.CreateAt(ctx, blobcache.Handle{}, "test", nil)
	require.NoError(t, err)
}

func newTestService(t *testing.T) *bclocal.Service {
	dir := t.TempDir()
	blobDirPath := filepath.Join(dir, "blob")
	pebbleDirPath := filepath.Join(dir, "pebble")
	for _, dir := range []string{blobDirPath, pebbleDirPath} {
		require.NoError(t, os.MkdirAll(dir, 0o755))
	}

	db, err := pebble.Open(pebbleDirPath, &pebble.Options{})
	require.NoError(t, err)
	blobDir, err := os.OpenRoot(blobDirPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, blobDir.Close())
	})

	schemas := bclocal.DefaultSchemas()
	schemas["gotns"] = Schema{}
	rootSpec := blobcache.DefaultLocalSpec()
	rootSpec.Local.HashAlgo = blobcache.HashAlgo_BLAKE2b_256
	rootSpec.Local.Schema = "gotns"
	s := bclocal.New(bclocal.Env{
		DB:         db,
		BlobDir:    blobDir,
		Schemas:    schemas,
		Root:       rootSpec,
		PacketConn: testutil.PacketConn(t),
	}, bclocal.Config{})
	t.Cleanup(func() {
		// This is required to avoid panics when closing the database.
		s.AbortAll(testutil.Context(t))
	})
	return s
}

func newTestSigner(t *testing.T) (sign.PublicKey, sign.PrivateKey) {
	pub, priv := DeriveSign([32]byte{})
	return pub, priv
}

func newTestKEM(_ *testing.T) (kem.PublicKey, kem.PrivateKey) {
	pub, priv := DeriveKEM([64]byte{})
	return pub, priv
}
