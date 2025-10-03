package reposchema

import (
	"bytes"
	"context"
	"encoding/hex"

	"blobcache.io/blobcache/src/blobcache"

	"github.com/gotvc/got/src/gotkv"
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

// Namespace return the namespace.
func (c *Client) Namespace(ctx context.Context) (*blobcache.Handle, error) {
	rootH, err := c.rootHandle(ctx)
	if err != nil {
		return nil, err
	}
	txn, err := blobcache.BeginTx(ctx, c.Service, *rootH, blobcache.TxParams{Mutate: true})
	if err != nil {
		return nil, err
	}
	defer txn.Abort(ctx)

	root, err := c.getRoot(ctx, txn)
	if err != nil {
		return nil, err
	}
	subVolID, err := c.getNS(ctx, txn, *root)
	if err != nil {
		return nil, err
	}
	if subVolID != nil {
		// ns exists, return it.
		return c.Service.OpenFrom(ctx, *rootH, *subVolID, blobcache.Action_ALL)
	}

	// ns doesn't exist, create it.
	subVol, err := c.createSubVolume(ctx, txn, GotNSVolumeSpec())
	if err != nil {
		return nil, err
	}
	root, err = c.putNS(ctx, txn, *root, subVol.OID)
	if err != nil {
		return nil, err
	}
	if err := txn.Save(ctx, root.Marshal(nil)); err != nil {
		return nil, err
	}
	if err := txn.Commit(ctx); err != nil {
		return nil, err
	}
	return subVol, nil
}

// StagingArea returns a handle to a staging area, creating it if it doesn't exist.
// StagingAreas are volumes where data is initially imported when it is added to Got.
// There are separate Volumes.
func (c *Client) StagingArea(ctx context.Context, paramHash *[32]byte) (*blobcache.Handle, error) {
	rootH, err := c.rootHandle(ctx)
	if err != nil {
		return nil, err
	}
	txn, err := blobcache.BeginTx(ctx, c.Service, *rootH, blobcache.TxParams{Mutate: true})
	if err != nil {
		return nil, err
	}
	defer txn.Abort(ctx)

	root, err := c.getRoot(ctx, txn)
	if err != nil {
		return nil, err
	}
	stageOID, err := c.getStage(ctx, txn, *root, paramHash)
	if err != nil {
		return nil, err
	}
	if stageOID != nil {
		// stage exists, return it.
		return c.Service.OpenFrom(ctx, *rootH, *stageOID, blobcache.Action_ALL)
	}

	// stage doesn't exist, create it.
	subVol, err := c.createSubVolume(ctx, txn, StageVolumeSpec())
	if err != nil {
		return nil, err
	}
	root, err = c.putStage(ctx, txn, *root, paramHash, subVol.OID)
	if err != nil {
		return nil, err
	}
	if err := txn.Save(ctx, root.Marshal(nil)); err != nil {
		return nil, err
	}
	if err := txn.Commit(ctx); err != nil {
		return nil, err
	}
	return subVol, nil
}

func (c *Client) ForEachStage(ctx context.Context, fn func(paramHash *[32]byte, volid blobcache.OID) error) error {
	rootH, err := c.rootHandle(ctx)
	if err != nil {
		return err
	}
	txn, err := blobcache.BeginTx(ctx, c.Service, *rootH, blobcache.TxParams{Mutate: false})
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
func (c *Client) createSubVolume(ctx context.Context, txn *blobcache.Tx, spec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	subVol, err := c.Service.CreateVolume(ctx, nil, spec)
	if err != nil {
		return nil, err
	}
	if err := txn.AllowLink(ctx, *subVol); err != nil {
		return nil, err
	}
	return subVol, nil
}

func (c *Client) rootHandle(ctx context.Context) (*blobcache.Handle, error) {
	return c.Service.OpenFiat(ctx, blobcache.OID{}, blobcache.Action_ALL)
}

func (c *Client) getRoot(ctx context.Context, txn *blobcache.Tx) (*gotkv.Root, error) {
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
func (c *Client) getStage(ctx context.Context, s stores.Reading, root gotkv.Root, paramHash *[32]byte) (*blobcache.OID, error) {
	val, err := c.GotKV.Get(ctx, s, root, stageKey(paramHash))
	if err != nil {
		if gotkv.IsErrKeyNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	var subVolID blobcache.OID
	if err := subVolID.Unmarshal(val); err != nil {
		return nil, err
	}
	return &subVolID, nil
}

func (c *Client) putStage(ctx context.Context, s stores.RW, root gotkv.Root, paramHash *[32]byte, subVolID blobcache.OID) (*gotkv.Root, error) {
	return c.GotKV.Put(ctx, s, root, stageKey(paramHash), subVolID.Marshal(nil))
}

// getNS returns the Namespace Volume OID if it exists, or (nil, nil) if it doesn't.
func (c *Client) getNS(ctx context.Context, s stores.Reading, root gotkv.Root) (*blobcache.OID, error) {
	val, err := c.GotKV.Get(ctx, s, root, nsKey)
	if err != nil {
		if gotkv.IsErrKeyNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	var subVolID blobcache.OID
	if err := subVolID.Unmarshal(val); err != nil {
		return nil, err
	}
	return &subVolID, nil
}

func (c *Client) putNS(ctx context.Context, s stores.RW, root gotkv.Root, subVolID blobcache.OID) (*gotkv.Root, error) {
	return c.GotKV.Put(ctx, s, root, nsKey, subVolID.Marshal(nil))
}

func stageKey(paramHash *[32]byte) []byte {
	return []byte("stage/" + hex.EncodeToString(paramHash[:]))
}

var nsKey = []byte("ns")

func GotRepoVolumeSpec() blobcache.VolumeSpec {
	return blobcache.VolumeSpec{
		Local: &blobcache.VolumeBackend_Local{
			VolumeParams: blobcache.VolumeParams{
				Schema:   SchemaName_GotRepo,
				HashAlgo: blobcache.HashAlgo_BLAKE2b_256,
				MaxSize:  1 << 22,
				Salted:   false,
			},
		},
	}
}

func GotNSVolumeSpec() blobcache.VolumeSpec {
	return blobcache.VolumeSpec{
		Local: &blobcache.VolumeBackend_Local{
			VolumeParams: blobcache.VolumeParams{
				Schema:   SchemaName_GotNS,
				HashAlgo: blobcache.HashAlgo_BLAKE2b_256,
				MaxSize:  1 << 22,
				Salted:   false,
			},
		},
	}
}

func StageVolumeSpec() blobcache.VolumeSpec {
	return blobcache.VolumeSpec{
		Local: &blobcache.VolumeBackend_Local{
			VolumeParams: blobcache.VolumeParams{
				Schema:   blobcache.Schema_NONE,
				HashAlgo: blobcache.HashAlgo_BLAKE2b_256,
				MaxSize:  1 << 22,
				Salted:   false,
			},
		},
	}
}
