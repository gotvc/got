package reposchema

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.brendoncarroll.net/tai64"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/gotns"
	"github.com/gotvc/got/src/internal/stores"
)

type Client struct {
	bcsvc blobcache.Service
	gotkv gotkv.Machine
	dmach gdat.Machine
}

func NewClient(svc blobcache.Service) Client {
	mach := gotkv.NewMachine(gotkv.Params{MeanSize: 1 << 14, MaxSize: 1 << 22})
	return Client{
		bcsvc: svc,
		gotkv: mach,
		dmach: *gdat.NewMachine(gdat.Params{}),
	}
}

// GetNamespace returns a handle to the namespace volume, creating it if it doesn't exist.
// It does not modify the contents of the namespace volume.
func (c *Client) GetNamespace(ctx context.Context, repoVol blobcache.OID, useSchema bool) (*blobcache.Handle, *gdat.DEK, error) {
	rootH, err := c.rootHandle(ctx, repoVol)
	if err != nil {
		return nil, nil, err
	}
	txn, err := bcsdk.BeginTx(ctx, c.bcsvc, *rootH, blobcache.TxParams{Modify: true})
	if err != nil {
		return nil, nil, err
	}
	defer txn.Abort(ctx)

	root, err := c.getRoot(ctx, txn)
	if err != nil {
		return nil, nil, err
	}
	sve, err := c.getNS(ctx, txn, root)
	if err != nil {
		return nil, nil, err
	}
	if sve != nil {
		// ns exists, return it.
		volh, err := c.bcsvc.OpenFrom(ctx, *rootH, sve.Token, blobcache.Action_ALL)
		if err != nil {
			return nil, nil, err
		}
		return volh, &sve.Secret, nil
	}

	// ns doesn't exist, create it.
	nsSpec := gotns.SpaceVolumeSpec()
	if !useSchema {
		nsSpec.Local.Schema = blobcache.SchemaSpec{Name: blobcache.Schema_NONE}
	}
	subVol, ltok, err := c.createSubVolume(ctx, txn, nsSpec)
	if err != nil {
		return nil, nil, err
	}
	sve = &subvolEntry{
		Token:  *ltok,
		Secret: generateDEK(),
	}
	root, err = c.setNS(ctx, txn, root, *sve)
	if err != nil {
		return nil, nil, err
	}
	if err := txn.Save(ctx, root.Marshal(nil)); err != nil {
		return nil, nil, err
	}
	if err := txn.Commit(ctx); err != nil {
		return nil, nil, err
	}
	return subVol, &sve.Secret, nil
}

// StagingArea returns a handle to a staging area, creating it if it doesn't exist.
// StagingAreas are volumes where data is initially imported when it is added to Got.
// There are separate Volumes.
func (c *Client) StagingArea(ctx context.Context, repoVol blobcache.OID, wcid StageID) (*blobcache.Handle, *gdat.DEK, error) {
	rootH, err := c.rootHandle(ctx, repoVol)
	if err != nil {
		return nil, nil, err
	}
	txn, err := bcsdk.BeginTx(ctx, c.bcsvc, *rootH, blobcache.TxParams{Modify: true})
	if err != nil {
		return nil, nil, err
	}
	defer txn.Abort(ctx)

	root, err := c.getRoot(ctx, txn)
	if err != nil {
		return nil, nil, err
	}
	if sve, err := c.getStage(ctx, txn, root, wcid); err != nil {
		return nil, nil, err
	} else if sve != nil {
		return c.openSubvolume(ctx, *rootH, *sve)
	}

	// stage doesn't exist, create it.
	svh, svTok, err := c.createSubVolume(ctx, txn, StageVolumeSpec())
	if err != nil {
		return nil, nil, err
	}
	sve := subvolEntry{
		Token:  *svTok,
		Secret: generateDEK(),
	}
	root, err = c.putStage(ctx, txn, root, wcid, sve)
	if err != nil {
		return nil, nil, err
	}
	if err := txn.Save(ctx, root.Marshal(nil)); err != nil {
		return nil, nil, err
	}
	if err := txn.Commit(ctx); err != nil {
		return nil, nil, err
	}
	return svh, &sve.Secret, nil
}

func (c *Client) ForEachStage(ctx context.Context, repoVol blobcache.OID, fn func(paramHash *[32]byte, volid blobcache.OID) error) error {
	rootH, err := c.rootHandle(ctx, repoVol)
	if err != nil {
		return err
	}
	txn, err := bcsdk.BeginTx(ctx, c.bcsvc, *rootH, blobcache.TxParams{Modify: false})
	if err != nil {
		return err
	}
	defer txn.Abort(ctx)
	root, err := c.getRoot(ctx, txn)
	if err != nil {
		return err
	}
	span := gotkv.PrefixSpan([]byte("stage/"))
	return c.gotkv.ForEach(ctx, txn, root, span, func(ent gotkv.Entry) error {
		var paramHash [32]byte
		hexData := bytes.TrimPrefix(ent.Key, []byte("stage/"))
		if _, err := hex.Decode(hexData, paramHash[:]); err != nil {
			return err
		}
		var volid blobcache.OID
		if err := volid.Unmarshal(ent.Value); err != nil {
			return err
		}
		return fn(&paramHash, volid)
	})
}

// RepairRepoLinks refreshes all repo-schema link tokens in a single transaction.
// If any referenced subvolume cannot be reopened, the transaction is aborted.
func (c *Client) RepairRepoLinks(ctx context.Context, repoVol blobcache.OID) error {
	rootH, err := c.rootHandle(ctx, repoVol)
	if err != nil {
		return err
	}
	txn, err := bcsdk.BeginTx(ctx, c.bcsvc, *rootH, blobcache.TxParams{Modify: true})
	if err != nil {
		return err
	}
	defer txn.Abort(ctx)

	root, err := c.getRoot(ctx, txn)
	if err != nil {
		return err
	}

	type entry struct {
		Key []byte
		SVE subvolEntry
	}
	var entries []entry

	if sve, err := c.getNS(ctx, txn, root); err != nil {
		return err
	} else if sve != nil {
		entries = append(entries, entry{Key: nsKey, SVE: *sve})
	}

	span := gotkv.PrefixSpan([]byte("stage/"))
	if err := c.gotkv.ForEach(ctx, txn, root, span, func(ent gotkv.Entry) error {
		sve, err := parseSubvolEntry(ent.Value)
		if err != nil {
			return err
		}
		key := append([]byte(nil), ent.Key...)
		entries = append(entries, entry{Key: key, SVE: *sve})
		return nil
	}); err != nil {
		return err
	}

	for i := range entries {
		old := entries[i].SVE
		logctx.Infof(ctx, "replacing repo link key=%q target=%v", string(entries[i].Key), old.Token.Target)
		target, err := c.bcsvc.OpenFiat(ctx, old.Token.Target, old.Token.Rights)
		if err != nil {
			return err
		}
		ltok, err := txn.Link(ctx, *target, old.Token.Rights)
		if err != nil {
			return err
		}
		old.Token = *ltok
		root, err = c.gotkv.Put(ctx, txn, root, entries[i].Key, old.Marshal(nil))
		if err != nil {
			return err
		}
	}

	if err := txn.Save(ctx, root.Marshal(nil)); err != nil {
		return err
	}
	return txn.Commit(ctx)
}

// createSubVolume creates a new volume and calls txn.AllowLink so it can be persisted indefinitely
// by the root Volume's Schema.
func (c *Client) createSubVolume(ctx context.Context, txn *bcsdk.Tx, spec blobcache.VolumeSpec) (*blobcache.Handle, *blobcache.LinkToken, error) {
	subVol, err := c.bcsvc.CreateVolume(ctx, nil, spec)
	if err != nil {
		return nil, nil, err
	}
	ltok, err := txn.Link(ctx, *subVol, blobcache.Action_ALL)
	if err != nil {
		return nil, nil, err
	}
	return subVol, ltok, nil
}

func (c *Client) rootHandle(ctx context.Context, repoVol blobcache.OID) (*blobcache.Handle, error) {
	return c.bcsvc.OpenFiat(ctx, repoVol, blobcache.Action_ALL)
}

func (c *Client) getRoot(ctx context.Context, txn *bcsdk.Tx) (gotkv.Root, error) {
	var rootData []byte
	if err := txn.Load(ctx, &rootData); err != nil {
		return gotkv.Root{}, err
	}
	var root gotkv.Root
	if len(rootData) > 0 {
		if err := root.Unmarshal(rootData); err != nil {
			return gotkv.Root{}, err
		}
	} else {
		newRoot, err := c.gotkv.NewEmpty(ctx, txn)
		if err != nil {
			return gotkv.Root{}, err
		}
		root = newRoot
	}
	return root, nil
}

// getState returns the volume OID for a stage.
func (c *Client) getStage(ctx context.Context, s stores.RO, root gotkv.Root, wcid StageID) (*subvolEntry, error) {
	val, err := c.gotkv.Get(ctx, s, root, stageKey(wcid))
	if err != nil {
		if gotkv.IsErrKeyNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return parseSubvolEntry(val)
}

func (c *Client) putStage(ctx context.Context, s stores.RW, root gotkv.Root, wcid StageID, sve subvolEntry) (gotkv.Root, error) {
	return c.gotkv.Put(ctx, s, root, stageKey(wcid), sve.Marshal(nil))
}

// getNS returns the Namespace Volume OID if it exists, or (nil, nil) if it doesn't.
func (c *Client) getNS(ctx context.Context, s stores.RO, root gotkv.Root) (*subvolEntry, error) {
	val, err := c.gotkv.Get(ctx, s, root, nsKey)
	if err != nil {
		if gotkv.IsErrKeyNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return parseSubvolEntry(val)
}

func (c *Client) setNS(ctx context.Context, s stores.RW, root gotkv.Root, sve subvolEntry) (gotkv.Root, error) {
	return c.gotkv.Put(ctx, s, root, nsKey, sve.Marshal(nil))
}

func (c *Client) openSubvolume(ctx context.Context, rooth blobcache.Handle, sve subvolEntry) (*blobcache.Handle, *gdat.DEK, error) {
	// stage exists, return it.
	volh, err := c.bcsvc.OpenFrom(ctx, rooth, sve.Token, blobcache.Action_ALL)
	if err != nil {
		return nil, nil, err
	}
	return volh, &sve.Secret, nil
}

func stageKey(wcid StageID) []byte {
	return []byte("stage/" + hex.EncodeToString(wcid[:]))
}

var nsKey = []byte("ns")

func StageVolumeSpec() blobcache.VolumeSpec {
	return blobcache.VolumeSpec{
		Local: &blobcache.VolumeBackend_Local{
			Schema:   blobcache.SchemaSpec{Name: blobcache.Schema_NONE},
			HashAlgo: blobcache.HashAlgo_BLAKE2b_256,
			MaxSize:  1 << 21,
			Salted:   false,
		},
	}
}

func generateDEK() gdat.DEK {
	var secret gdat.DEK
	if n, err := rand.Read(secret[:]); err != nil {
		panic(err)
	} else if n != len(secret) {
		panic(n)
	}
	return secret
}

type subvolEntry struct {
	Token  blobcache.LinkToken
	Secret gdat.DEK
}

func parseSubvolEntry(data []byte) (*subvolEntry, error) {
	if len(data) != blobcache.LinkTokenSize+gdat.DEKSize {
		return nil, fmt.Errorf("value is wrong size for subvolume entry HAVE=%d", len(data))
	}
	var ret subvolEntry
	if err := ret.Token.Unmarshal(data[:blobcache.LinkTokenSize]); err != nil {
		return nil, err
	}
	copy(ret.Secret[:], data[blobcache.LinkTokenSize:])
	return &ret, nil
}

func (sve subvolEntry) Marshal(out []byte) []byte {
	out = sve.Token.Marshal(out)
	out = append(out, sve.Secret[:]...)
	return out
}

type StageID [16]byte

func NewStageID() (ret StageID) {
	copy(ret[:8], tai64.Now().TAI64().Marshal())
	rand.Read(ret[8:])
	return ret
}

func (sid StageID) MarshalText() ([]byte, error) {
	return []byte(hex.EncodeToString(sid[:])), nil
}

func (sid *StageID) UnmarshalText(data []byte) error {
	_, err := hex.Decode(sid[:], data)
	return err
}

func (sid StageID) String() string {
	data, _ := sid.MarshalText()
	return string(data)
}
