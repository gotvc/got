package gotns

import (
	"context"
	"errors"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/cloudflare/circl/kem"
	dilithium3 "github.com/cloudflare/circl/sign/dilithium/mode3"
	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/volumes"
	"go.brendoncarroll.net/exp/slices2"
	"go.inet256.org/inet256/src/inet256"
)

// Client holds configuration for accessing a GotNS instance backed by a Blobcache Volume.
type Client struct {
	Blobcache blobcache.Service
	Machine   Machine
	ActAs     LeafPrivate
}

// Init initializes a new GotNS instance in the given volume.
func (c *Client) Init(ctx context.Context, volh blobcache.Handle, admins []IdentityLeaf) error {
	volh, err := c.adjustHandle(ctx, volh)
	if err != nil {
		return err
	}
	tx, err := blobcache.BeginTx(ctx, c.Blobcache, volh, blobcache.TxParams{Mutate: true})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	var data []byte
	if err := tx.Load(ctx, &data); err != nil {
		return err
	}
	if len(data) > 0 {
		return errors.New("gotns: root already exists")
	}
	root, err := c.Machine.New(ctx, tx, admins)
	if err != nil {
		return err
	}
	if err := tx.Save(ctx, root.Marshal(nil)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Do calls fn with a transaction for manipulating the NS.
func (c *Client) Do(ctx context.Context, volh blobcache.Handle, fn func(txb *Txn) error) error {
	return c.doTx(ctx, volh, c.ActAs, fn)
}

func (c *Client) GetGroup(ctx context.Context, volh blobcache.Handle, name string) (*Group, error) {
	var group *Group
	if err := c.view(ctx, volh, func(s stores.Reading, state State) error {
		var err error
		group, err = c.Machine.GetGroup(ctx, s, state, name)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return group, nil
}

func (c *Client) GetLeaf(ctx context.Context, volh blobcache.Handle, id inet256.ID) (*IdentityLeaf, error) {
	var leaf *IdentityLeaf
	if err := c.view(ctx, volh, func(s stores.Reading, state State) error {
		var err error
		leaf, err = c.Machine.GetLeaf(ctx, s, state, id)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return leaf, nil
}

func (c *Client) GetEntry(ctx context.Context, volh blobcache.Handle, name string) (*Entry, error) {
	volh, err := c.adjustHandle(ctx, volh)
	if err != nil {
		return nil, err
	}
	tx, err := blobcache.BeginTx(ctx, c.Blobcache, volh, blobcache.TxParams{})
	if err != nil {
		return nil, err
	}
	defer tx.Abort(ctx)
	state, err := loadState(ctx, tx)
	if err != nil {
		return nil, err
	}
	return c.Machine.GetEntry(ctx, tx, *state, []byte(name))
}

func (c *Client) PutEntry(ctx context.Context, volh blobcache.Handle, ent Entry) error {
	return c.doTx(ctx, volh, c.ActAs, func(txb *Txn) error {
		return txb.PutEntry(ctx, ent)
	})
}

func (c *Client) DeleteEntry(ctx context.Context, volh blobcache.Handle, name string) error {
	return c.doTx(ctx, volh, c.ActAs, func(txb *Txn) error {
		return txb.DeleteEntry(ctx, name)
	})
}

func (c *Client) ListEntries(ctx context.Context, volh blobcache.Handle, span branches.Span, limit int) ([]string, error) {
	volh, err := c.adjustHandle(ctx, volh)
	if err != nil {
		return nil, err
	}
	tx, err := blobcache.BeginTx(ctx, c.Blobcache, volh, blobcache.TxParams{Mutate: false})
	if err != nil {
		return nil, err
	}
	defer tx.Abort(ctx)
	state, err := loadState(ctx, tx)
	if err != nil {
		return nil, err
	}
	entries, err := c.Machine.ListEntries(ctx, tx, *state, span, limit)
	if err != nil {
		return nil, err
	}
	names := slices2.Map(entries, func(e Entry) string {
		return e.Name
	})
	return names, nil
}

func (c *Client) Inspect(ctx context.Context, volh blobcache.Handle, name string) (*branches.Info, error) {
	volh, err := c.adjustHandle(ctx, volh)
	if err != nil {
		return nil, err
	}
	tx, err := blobcache.BeginTx(ctx, c.Blobcache, volh, blobcache.TxParams{Mutate: false})
	if err != nil {
		return nil, err
	}
	defer tx.Abort(ctx)
	return nil, nil
}

func (c *Client) CreateAt(ctx context.Context, nsh blobcache.Handle, name string, aux []byte) error {
	if c.ActAs == (LeafPrivate{}) {
		return errors.New("gotns.Client: ActAs cannot be empty")
	}
	nsh, err := c.adjustHandle(ctx, nsh)
	if err != nil {
		return err
	}
	tx, err := blobcache.BeginTx(ctx, c.Blobcache, nsh, blobcache.TxParams{Mutate: true})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	subVolh, err := c.createSubVolume(ctx, tx)
	if err != nil {
		return err
	}
	var rootData []byte
	if err := tx.Load(ctx, &rootData); err != nil {
		return err
	}
	root, err := ParseRoot(rootData)
	if err != nil {
		return err
	}
	tx2 := c.Machine.NewTxn(root, tx, []LeafPrivate{c.ActAs})
	if err := tx2.PutEntry(ctx, Entry{
		Name:   name,
		Volume: subVolh.OID,
		Rights: blobcache.Action_ALL,
	}); err != nil {
		return err
	}
	root2, err := tx2.Finish(ctx)
	if err != nil {
		return err
	}
	if err := tx.Save(ctx, root2.Marshal(nil)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (c *Client) OpenAt(ctx context.Context, nsh blobcache.Handle, name string) (branches.Volume, error) {
	nsh, err := c.adjustHandle(ctx, nsh)
	if err != nil {
		return nil, err
	}
	ent, err := c.GetEntry(ctx, nsh, name)
	if err != nil {
		return nil, err
	}
	if ent == nil {
		return nil, branches.ErrNotExist
	}
	volh, err := c.Blobcache.OpenFrom(ctx, nsh, ent.Volume, blobcache.Action_ALL)
	if err != nil {
		return nil, err
	}
	innerVol := &volumes.Blobcache{
		Handle:  *volh,
		Service: c.Blobcache,
	}
	// TODO: get the secret from the entry.
	var secret [32]byte
	pubKey, privKey := dilithium3.Scheme().DeriveKey(secret[:])
	signedVol := volumes.NewSignedVolume(innerVol, pubKey, privKey)
	secret2 := [32]byte(gdat.Hash(secret[:]))
	vol := volumes.NewChaCha20Poly1305(signedVol, &secret2)
	return vol, nil
}

// AddLeaf adds a new primitive identity to a group.
func (c *Client) AddLeaf(ctx context.Context, volh blobcache.Handle, leaf IdentityLeaf) error {
	panic("not implemented")
}

// AddMember adds a named identity to a group.
func (c *Client) AddMember(ctx context.Context, volh blobcache.Handle, name string, member string) error {
	volh, err := c.adjustHandle(ctx, volh)
	if err != nil {
		return err
	}
	tx, err := blobcache.BeginTx(ctx, c.Blobcache, volh, blobcache.TxParams{Mutate: true})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	root, err := loadState(ctx, tx)
	if err != nil {
		return err
	}
	root, err = c.Machine.AddMember(ctx, tx, *root, name, member)
	if err != nil {
		return err
	}
	if err := tx.Save(ctx, root.Marshal(nil)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (c *Client) adjustHandle(ctx context.Context, volh blobcache.Handle) (blobcache.Handle, error) {
	if volh.Secret == ([16]byte{}) {
		volh, err := c.Blobcache.OpenAs(ctx, nil, volh.OID, blobcache.Action_ALL)
		if err != nil {
			return blobcache.Handle{}, err
		}
		return *volh, nil
	} else {
		return volh, nil
	}
}

func (c *Client) doTx(ctx context.Context, volh blobcache.Handle, leafPriv LeafPrivate, fn func(txb *Txn) error) error {
	if c.ActAs == (LeafPrivate{}) {
		return errors.New("gotns.Client: ActAs cannot be nil")
	}
	volh, err := c.adjustHandle(ctx, volh)
	if err != nil {
		return err
	}
	tx, err := blobcache.BeginTx(ctx, c.Blobcache, volh, blobcache.TxParams{Mutate: true})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	var data []byte
	if err := tx.Load(ctx, &data); err != nil {
		return err
	}
	root, err := ParseRoot(data)
	if err != nil {
		return err
	}
	builder := c.Machine.NewTxn(root, tx, []LeafPrivate{leafPriv})
	if err := fn(builder); err != nil {
		return err
	}
	root2, err := builder.Finish(ctx)
	if err != nil {
		return err
	}
	if err := tx.Save(ctx, root2.Marshal(nil)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (c *Client) view(ctx context.Context, volh blobcache.Handle, fn func(s stores.Reading, state State) error) error {
	volh, err := c.adjustHandle(ctx, volh)
	if err != nil {
		return err
	}
	tx, err := blobcache.BeginTx(ctx, c.Blobcache, volh, blobcache.TxParams{Mutate: false})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	state, err := loadState(ctx, tx)
	if err != nil {
		return err
	}
	return fn(tx, *state)
}

func (c *Client) createSubVolume(ctx context.Context, tx *blobcache.Tx) (*blobcache.Handle, error) {
	volh, err := c.Blobcache.CreateVolume(ctx, nil, BranchVolumeSpec())
	if err != nil {
		return nil, err
	}
	if err := tx.AllowLink(ctx, *volh); err != nil {
		return nil, err
	}
	return volh, nil
}

// IntroduceSelf creates a signed change set that adds a leaf to the state.
// Then it returns the signed change set data.
// It does not contact Blobcache or perform any Volume operations.
func (c *Client) IntroduceSelf(kemPub kem.PublicKey) Op_ChangeSet {
	leaf := NewLeaf(c.ActAs.SigPrivateKey.Public().(inet256.PublicKey), kemPub)
	cs := Op_ChangeSet{
		Ops: []Op{
			&Op_CreateLeaf{
				Leaf: leaf,
			},
		},
	}
	cs.Sign(c.ActAs.SigPrivateKey)
	return cs
}

func loadState(ctx context.Context, tx *blobcache.Tx) (*State, error) {
	var data []byte
	if err := tx.Load(ctx, &data); err != nil {
		return nil, err
	}
	root, err := ParseRoot(data)
	if err != nil {
		return nil, err
	}
	return &root.State, nil
}

// BranchVolumeSpec is the volume spec used for new branches.
func BranchVolumeSpec() blobcache.VolumeSpec {
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
