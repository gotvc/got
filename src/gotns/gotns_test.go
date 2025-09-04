package gotns

import (
	"regexp"
	"strconv"
	"testing"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/simplens"
	"github.com/cloudflare/circl/kem"
	"github.com/cloudflare/circl/sign"
	"github.com/cloudflare/circl/sign/ed25519"
	"github.com/stretchr/testify/require"
	"go.inet256.org/inet256/src/inet256"

	"github.com/gotvc/got/src/internal/dbutil"
	"github.com/gotvc/got/src/internal/testutil"
)

func TestInit(t *testing.T) {
	ctx := testutil.Context(t)
	bc := bclocal.NewTestService(t)
	nsc := simplens.Client{Service: bc}
	volh, err := nsc.CreateAt(ctx, blobcache.Handle{}, "test", blobcache.DefaultLocalSpec())
	require.NoError(t, err)

	signPub, sigPriv := newTestSigner(t)
	gnsc := Client{Blobcache: bc, Machine: New(), ActAs: sigPriv}
	kemPub, _ := newTestKEM(t)
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
		&Op_AddRule{Rule: Rule{Subject: "sub", Verb: "verb", Object: NewGroupSet(regexp.MustCompile(".*"))}},
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
	kemPub, _ := newTestKEM(t)
	gnsc := Client{Blobcache: bc, Machine: New(), ActAs: sigPriv}
	require.NoError(t, gnsc.Init(ctx, blobcache.Handle{}, []IdentityLeaf{NewLeaf(sigPub, kemPub)}))

	err := gnsc.CreateAt(ctx, blobcache.Handle{}, "test", nil)
	require.NoError(t, err)
}

func newTestService(t *testing.T) *bclocal.Service {
	ctx := testutil.Context(t)
	db := dbutil.NewTestDB(t)
	require.NoError(t, bclocal.SetupDB(ctx, db))
	schemas := bclocal.DefaultSchemas()
	schemas["gotns"] = Schema{}
	rootSpec := blobcache.DefaultLocalSpec()
	rootSpec.Local.HashAlgo = blobcache.HashAlgo_BLAKE2b_256
	rootSpec.Local.Schema = "gotns"
	return bclocal.New(bclocal.Env{
		DB:         db,
		Schemas:    schemas,
		Root:       rootSpec,
		PacketConn: testutil.PacketConn(t),
	})
}

func newTestSigner(t *testing.T) (sign.PublicKey, sign.PrivateKey) {
	pub, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	return pub, priv
}

func newTestKEM(t *testing.T) (kem.PublicKey, kem.PrivateKey) {
	pub, priv, err := GenerateKEM()
	require.NoError(t, err)
	return pub, priv
}
