package reposchema

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"testing"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/gotorg"
	"github.com/gotvc/got/src/internal/gotbc"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.inet256.org/inet256/src/inet256"
)

func TestClient(t *testing.T) {
	ctx := testutil.Context(t)
	bc := newBlobcache(t)
	client := NewClient(bc)
	repoVol := blobcache.OID{}
	nsh, _, err := client.GetNamespace(ctx, repoVol, true)
	require.NoError(t, err)

	var vh blobcache.Handle
	for i := 0; i < 3; i++ {
		vh2, _, err := client.StagingArea(ctx, repoVol, StageID{})
		require.NoError(t, err)
		if vh != (blobcache.Handle{}) {
			// Check that the same OID comes back each time.
			require.Equal(t, vh.OID, vh2.OID)
		}
		vh = *vh2
	}
	if vh == (blobcache.Handle{}) {
		t.Fatal("vh is empty")
	}

	// Initialize GotNS
	gnsc := gotorg.Client{Blobcache: bc, Machine: gotorg.New(), ActAs: gotorg.IdenPrivate{}}
	require.NoError(t, gnsc.EnsureInit(ctx, *nsh, []gotorg.IdentityUnit{}))

	// Write some blobs to a staging area
	txn, err := bcsdk.BeginTx(ctx, bc, vh, blobcache.TxParams{Modify: true})
	require.NoError(t, err)
	defer txn.Abort(ctx)
	var cids []blobcache.CID
	for i := 0; i < 10; i++ {
		cid, err := txn.Post(ctx, fmt.Appendf(nil, "hello %d", i))
		require.NoError(t, err)
		cids = append(cids, cid)
	}
	require.NoError(t, txn.Commit(ctx))

	// Check that the blobs exist
	txn, err = bcsdk.BeginTx(ctx, bc, vh, blobcache.TxParams{})
	require.NoError(t, err)
	defer txn.Abort(ctx)
	for _, cid := range cids {
		yes, err := bcsdk.ExistsUnit(ctx, txn, cid)
		require.NoError(t, err)
		require.True(t, yes)
	}
}

func TestRepairRepoLinks(t *testing.T) {
	ctx := testutil.Context(t)
	bc := newBlobcache(t)
	client := NewClient(bc)
	repoVol := blobcache.OID{}

	nsh, _, err := client.GetNamespace(ctx, repoVol, true)
	require.NoError(t, err)
	wcid := NewStageID()
	stageh, _, err := client.StagingArea(ctx, repoVol, wcid)
	require.NoError(t, err)

	breakAllRepoLinks(t, ctx, &client, repoVol)

	_, _, err = client.GetNamespace(ctx, repoVol, true)
	require.Error(t, err)
	_, _, err = client.StagingArea(ctx, repoVol, wcid)
	require.Error(t, err)

	require.NoError(t, client.RepairRepoLinks(ctx, repoVol))

	nsh2, _, err := client.GetNamespace(ctx, repoVol, true)
	require.NoError(t, err)
	require.Equal(t, nsh.OID, nsh2.OID)

	stageh2, _, err := client.StagingArea(ctx, repoVol, wcid)
	require.NoError(t, err)
	require.Equal(t, stageh.OID, stageh2.OID)
}

func TestRepairRepoLinksFailAtomic(t *testing.T) {
	ctx := testutil.Context(t)
	bc := newBlobcache(t)
	client := NewClient(bc)
	repoVol := blobcache.OID{}

	_, _, err := client.GetNamespace(ctx, repoVol, true)
	require.NoError(t, err)
	wcid := NewStageID()
	_, _, err = client.StagingArea(ctx, repoVol, wcid)
	require.NoError(t, err)

	nsBefore := readSubvolEntry(t, ctx, &client, repoVol, nsKey)
	tamperStageTarget(t, ctx, &client, repoVol, wcid, randomOID(t))

	_, _, err = client.StagingArea(ctx, repoVol, wcid)
	require.Error(t, err)

	err = client.RepairRepoLinks(ctx, repoVol)
	require.Error(t, err)

	nsAfter := readSubvolEntry(t, ctx, &client, repoVol, nsKey)
	require.Equal(t, nsBefore.Marshal(nil), nsAfter.Marshal(nil))

	_, _, err = client.GetNamespace(ctx, repoVol, true)
	require.NoError(t, err)
	_, _, err = client.StagingArea(ctx, repoVol, wcid)
	require.Error(t, err)
}

func TestIdentityRoundTrip(t *testing.T) {
	ctx := testutil.Context(t)
	bc := newBlobcache(t)
	client := NewClient(bc)
	repoVol := blobcache.OID{}

	idp := gotorg.GenerateIden()
	id, err := client.PostIdentity(ctx, repoVol, idp)
	require.NoError(t, err)
	require.Equal(t, idp.GetID(), id)

	idp2, err := client.GetIdentity(ctx, repoVol, id)
	require.NoError(t, err)
	require.Equal(t, id, idp2.GetID())

	before, err := MarshalIden(idp)
	require.NoError(t, err)
	after, err := MarshalIden(*idp2)
	require.NoError(t, err)
	require.Equal(t, before, after)

	refData := readRootValue(t, ctx, &client, repoVol, idenKey(id))
	ref, err := gdat.ParseRef(refData)
	require.NoError(t, err)
	require.False(t, ref.IsZero())
}

func TestGetIdentityNotFound(t *testing.T) {
	ctx := testutil.Context(t)
	bc := newBlobcache(t)
	client := NewClient(bc)
	repoVol := blobcache.OID{}

	_, err := client.GetIdentity(ctx, repoVol, randomID(t))
	require.Error(t, err)
}

func TestPostIdentityIdempotent(t *testing.T) {
	ctx := testutil.Context(t)
	bc := newBlobcache(t)
	client := NewClient(bc)
	repoVol := blobcache.OID{}

	idp := gotorg.GenerateIden()
	id, err := client.PostIdentity(ctx, repoVol, idp)
	require.NoError(t, err)
	before := readRootValue(t, ctx, &client, repoVol, idenKey(id))

	id2, err := client.PostIdentity(ctx, repoVol, idp)
	require.NoError(t, err)
	require.Equal(t, id, id2)
	after := readRootValue(t, ctx, &client, repoVol, idenKey(id))
	require.Equal(t, before, after)
}

func TestConfigRoundTrip(t *testing.T) {
	ctx := testutil.Context(t)
	bc := newBlobcache(t)
	client := NewClient(bc)
	repoVol := blobcache.OID{}

	cfg, err := client.GetConfig(ctx, repoVol)
	require.NoError(t, err)
	require.Nil(t, cfg)

	err = client.EditConfig(ctx, repoVol, func(prev json.RawMessage) json.RawMessage {
		require.Nil(t, prev)
		return json.RawMessage(`{"hello":"world"}`)
	})
	require.NoError(t, err)

	cfg, err = client.GetConfig(ctx, repoVol)
	require.NoError(t, err)
	require.JSONEq(t, `{"hello":"world"}`, string(cfg))

	err = client.EditConfig(ctx, repoVol, func(prev json.RawMessage) json.RawMessage {
		require.JSONEq(t, `{"hello":"world"}`, string(prev))
		return nil
	})
	require.NoError(t, err)

	cfg, err = client.GetConfig(ctx, repoVol)
	require.NoError(t, err)
	require.Nil(t, cfg)
}

func breakAllRepoLinks(t testing.TB, ctx context.Context, c *Client, repoVol blobcache.OID) {
	t.Helper()
	rootH, err := c.rootHandle(ctx, repoVol)
	require.NoError(t, err)
	tx, err := bcsdk.BeginTx(ctx, c.bcsvc, *rootH, blobcache.TxParams{Modify: true})
	require.NoError(t, err)
	defer tx.Abort(ctx)

	root, err := c.getRoot(ctx, tx)
	require.NoError(t, err)

	var ltoks []blobcache.LinkToken
	if sve, err := c.getNS(ctx, tx, root); err != nil {
		require.NoError(t, err)
	} else if sve != nil {
		ltoks = append(ltoks, sve.Token)
	}

	span := gotkv.PrefixSpan([]byte("stage/"))
	err = c.gotkv.ForEach(ctx, tx, root, span, func(ent gotkv.Entry) error {
		sve, err := parseSubvolEntry(ent.Value)
		if err != nil {
			return err
		}
		ltoks = append(ltoks, sve.Token)
		return nil
	})
	require.NoError(t, err)

	ids := make([]blobcache.LinkTokenID, len(ltoks))
	for i := range ltoks {
		ids[i] = ltoks[i].GetID(tx.HashAlgo())
	}
	require.NoError(t, tx.Unlink(ctx, ids))
	require.NoError(t, tx.Commit(ctx))
}

func tamperStageTarget(t testing.TB, ctx context.Context, c *Client, repoVol blobcache.OID, wcid StageID, target blobcache.OID) {
	t.Helper()
	rootH, err := c.rootHandle(ctx, repoVol)
	require.NoError(t, err)
	tx, err := bcsdk.BeginTx(ctx, c.bcsvc, *rootH, blobcache.TxParams{Modify: true})
	require.NoError(t, err)
	defer tx.Abort(ctx)

	root, err := c.getRoot(ctx, tx)
	require.NoError(t, err)

	val, err := c.gotkv.Get(ctx, tx, root, stageKey(wcid))
	require.NoError(t, err)
	sve, err := parseSubvolEntry(val)
	require.NoError(t, err)
	sve.Token.Target = target

	root, err = c.gotkv.Put(ctx, tx, root, stageKey(wcid), sve.Marshal(nil))
	require.NoError(t, err)
	require.NoError(t, tx.Save(ctx, root.Marshal(nil)))
	require.NoError(t, tx.Commit(ctx))
}

func readSubvolEntry(t testing.TB, ctx context.Context, c *Client, repoVol blobcache.OID, key []byte) subvolEntry {
	t.Helper()
	rootH, err := c.rootHandle(ctx, repoVol)
	require.NoError(t, err)
	tx, err := bcsdk.BeginTx(ctx, c.bcsvc, *rootH, blobcache.TxParams{Modify: false})
	require.NoError(t, err)
	defer tx.Abort(ctx)

	root, err := c.getRoot(ctx, tx)
	require.NoError(t, err)
	val, err := c.gotkv.Get(ctx, tx, root, key)
	require.NoError(t, err)
	sve, err := parseSubvolEntry(val)
	require.NoError(t, err)
	return *sve
}

func readRootValue(t testing.TB, ctx context.Context, c *Client, repoVol blobcache.OID, key []byte) []byte {
	t.Helper()
	rootH, err := c.rootHandle(ctx, repoVol)
	require.NoError(t, err)
	tx, err := bcsdk.BeginTx(ctx, c.bcsvc, *rootH, blobcache.TxParams{Modify: false})
	require.NoError(t, err)
	defer tx.Abort(ctx)

	root, err := c.getRoot(ctx, tx)
	require.NoError(t, err)
	val, err := c.gotkv.Get(ctx, tx, root, key)
	require.NoError(t, err)
	return val
}

func randomOID(t testing.TB) (ret blobcache.OID) {
	t.Helper()
	if _, err := rand.Read(ret[:]); err != nil {
		t.Fatal(err)
	}
	return ret
}

func randomID(t testing.TB) (ret inet256.ID) {
	t.Helper()
	if _, err := rand.Read(ret[:]); err != nil {
		t.Fatal(err)
	}
	return ret
}

func newBlobcache(t testing.TB) blobcache.Service {
	env := bclocal.NewTestEnv(t)
	env.MkSchema = func(spec blobcache.SchemaSpec) (schema.Schema, error) {
		switch spec.Name {
		case SchemaName_GotRepo:
			return Constructor(spec.Params, nil)
		case SchemeName_GotOrg:
			return gotorg.SchemaConstructor(spec.Params, nil)
		case "":
			return schema.NoneConstructor(spec.Params, nil)
		default:
			return nil, fmt.Errorf("unknown schema %q", spec.Name)
		}
	}
	env.Root = gotbc.GotVolumeSpec()
	return bclocal.NewTestServiceFromEnv(t, env)
}
