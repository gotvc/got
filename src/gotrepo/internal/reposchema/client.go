package reposchema

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/tai64"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/gotns"
	"github.com/gotvc/got/src/internal/stores"
)

type Client struct {
	Service blobcache.Service
	GotKV   gotkv.Machine
}

func NewClient(svc blobcache.Service) Client {
	mach := gotkv.NewMachine(1<<14, 1<<22)
	return Client{
		Service: svc,
		GotKV:   mach,
	}
}

// GetNamespace returns a handle to the namespace volume, creating it if it doesn't exist.
// It does not modify the contents of the namespace volume.
func (c *Client) GetNamespace(ctx context.Context, repoVol blobcache.OID, useSchema bool) (*blobcache.Handle, *gdat.DEK, error) {
	rootH, err := c.rootHandle(ctx, repoVol)
	if err != nil {
		return nil, nil, err
	}
	txn, err := bcsdk.BeginTx(ctx, c.Service, *rootH, blobcache.TxParams{Modify: true})
	if err != nil {
		return nil, nil, err
	}
	defer txn.Abort(ctx)

	root, err := c.getRoot(ctx, txn)
	if err != nil {
		return nil, nil, err
	}
	sve, err := c.getNS(ctx, txn, *root)
	if err != nil {
		return nil, nil, err
	}
	if sve != nil {
		// ns exists, return it.
		volh, err := c.Service.OpenFrom(ctx, *rootH, sve.Token, blobcache.Action_ALL)
		if err != nil {
			return nil, nil, err
		}
		return volh, &sve.Secret, nil
	}

	// ns doesn't exist, create it.
	nsSpec := gotns.DefaultVolumeSpec()
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
	root, err = c.setNS(ctx, txn, *root, *sve)
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
	txn, err := bcsdk.BeginTx(ctx, c.Service, *rootH, blobcache.TxParams{Modify: true})
	if err != nil {
		return nil, nil, err
	}
	defer txn.Abort(ctx)

	root, err := c.getRoot(ctx, txn)
	if err != nil {
		return nil, nil, err
	}
	if sve, err := c.getStage(ctx, txn, *root, wcid); err != nil {
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
	root, err = c.putStage(ctx, txn, *root, wcid, sve)
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
	txn, err := bcsdk.BeginTx(ctx, c.Service, *rootH, blobcache.TxParams{Modify: false})
	if err != nil {
		return err
	}
	defer txn.Abort(ctx)
	root, err := c.getRoot(ctx, txn)
	if err != nil {
		return err
	}
	span := gotkv.PrefixSpan([]byte("stage/"))
	return c.GotKV.ForEach(ctx, txn, *root, span, func(ent gotkv.Entry) error {
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

// createSubVolume creates a new volume and calls txn.AllowLink so it can be persisted indefinitely
// by the root Volume's Schema.
func (c *Client) createSubVolume(ctx context.Context, txn *bcsdk.Tx, spec blobcache.VolumeSpec) (*blobcache.Handle, *blobcache.LinkToken, error) {
	subVol, err := c.Service.CreateVolume(ctx, nil, spec)
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
	return c.Service.OpenFiat(ctx, repoVol, blobcache.Action_ALL)
}

func (c *Client) getRoot(ctx context.Context, txn *bcsdk.Tx) (*gotkv.Root, error) {
	var rootData []byte
	if err := txn.Load(ctx, &rootData); err != nil {
		return nil, err
	}
	var root *gotkv.Root
	if len(rootData) > 0 {
		root = new(gotkv.Root)
		if err := root.Unmarshal(rootData); err != nil {
			return nil, err
		}
	} else {
		newRoot, err := c.GotKV.NewEmpty(ctx, txn)
		if err != nil {
			return nil, err
		}
		root = newRoot
	}
	return root, nil
}

// getState returns the volume OID for a stage.
func (c *Client) getStage(ctx context.Context, s stores.Reading, root gotkv.Root, wcid StageID) (*subvolEntry, error) {
	val, err := c.GotKV.Get(ctx, s, root, stageKey(wcid))
	if err != nil {
		if gotkv.IsErrKeyNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return parseSubvolEntry(val)
}

func (c *Client) putStage(ctx context.Context, s stores.RW, root gotkv.Root, wcid StageID, sve subvolEntry) (*gotkv.Root, error) {
	return c.GotKV.Put(ctx, s, root, stageKey(wcid), sve.Marshal(nil))
}

// getNS returns the Namespace Volume OID if it exists, or (nil, nil) if it doesn't.
func (c *Client) getNS(ctx context.Context, s stores.Reading, root gotkv.Root) (*subvolEntry, error) {
	val, err := c.GotKV.Get(ctx, s, root, nsKey)
	if err != nil {
		if gotkv.IsErrKeyNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return parseSubvolEntry(val)
}

func (c *Client) setNS(ctx context.Context, s stores.RW, root gotkv.Root, sve subvolEntry) (*gotkv.Root, error) {
	return c.GotKV.Put(ctx, s, root, nsKey, sve.Marshal(nil))
}

func (c *Client) openSubvolume(ctx context.Context, rooth blobcache.Handle, sve subvolEntry) (*blobcache.Handle, *gdat.DEK, error) {
	// stage exists, return it.
	volh, err := c.Service.OpenFrom(ctx, rooth, sve.Token, blobcache.Action_ALL)
	if err != nil {
		return nil, nil, err
	}
	return volh, &sve.Secret, nil
}

func stageKey(wcid StageID) []byte {
	return []byte("stage/" + hex.EncodeToString(wcid[:]))
}

var nsKey = []byte("ns")

func GotRepoVolumeSpec() blobcache.VolumeSpec {
	return blobcache.VolumeSpec{
		Local: &blobcache.VolumeBackend_Local{
			Schema:   blobcache.SchemaSpec{Name: SchemaName_GotRepo},
			HashAlgo: blobcache.HashAlgo_BLAKE2b_256,
			MaxSize:  1 << 21,
			Salted:   false,
		},
	}
}

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
