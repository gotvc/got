package gotns

import (
	"context"
	"crypto/rand"
	"errors"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/statetrace"
	"github.com/cloudflare/circl/kem"
	dilithium3 "github.com/cloudflare/circl/sign/dilithium/mode3"
	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotns/internal/gotnsop"
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

// EnsureInit initializes a new GotNS instance in the given volume.
// If the volume already contains a GotNS instance, it is left unchanged.
func (c *Client) EnsureInit(ctx context.Context, volh blobcache.Handle, admins []IdentityLeaf) error {
	volh, err := c.adjustHandle(ctx, volh)
	if err != nil {
		return err
	}
	tx, err := bcsdk.BeginTx(ctx, c.Blobcache, volh, blobcache.TxParams{Mutate: true})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	var data []byte
	if err := tx.Load(ctx, &data); err != nil {
		return err
	}
	if len(data) > 0 {
		if _, err := statetrace.Parse(data, ParseRoot); err != nil {
			return err
		}
		return nil
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
	return c.doTx(ctx, volh, c.ActAs, func(tx *bcsdk.Tx, txn *Txn) error {
		return fn(txn)
	})
}

func (c *Client) GetGroup(ctx context.Context, volh blobcache.Handle, name string) (*gotnsop.Group, error) {
	var group *gotnsop.Group
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

func (c *Client) GetBranch(ctx context.Context, volh blobcache.Handle, name string) (*BranchEntry, error) {
	volh, err := c.adjustHandle(ctx, volh)
	if err != nil {
		return nil, err
	}
	tx, err := bcsdk.BeginTx(ctx, c.Blobcache, volh, blobcache.TxParams{})
	if err != nil {
		return nil, err
	}
	defer tx.Abort(ctx)
	state, err := loadState(ctx, tx)
	if err != nil {
		return nil, err
	}
	return c.Machine.GetBranch(ctx, tx, *state, name)
}

// CreateBranch creates a new branch with a new volume at the specified name.
func (c *Client) CreateBranch(ctx context.Context, nsh blobcache.Handle, name string, aux []byte) error {
	return c.doTx(ctx, nsh, c.ActAs, func(tx *bcsdk.Tx, txn *Txn) error {
		svh, err := c.createSubVolume(ctx, tx)
		if err != nil {
			return err
		}
		if err := tx.Link(ctx, *svh, blobcache.Action_ALL); err != nil {
			return err
		}
		sec := [32]byte{}
		if _, err := rand.Read(sec[:]); err != nil {
			return err
		}
		hos := gdat.Hash(sec[:])
		if err := txn.AddVolume(ctx, VolumeEntry{
			Volume:       svh.OID,
			HashOfSecret: hos,
		}); err != nil {
			return err
		}
		if err := txn.PutBranch(ctx, gotnsop.BranchEntry{
			Name:   name,
			Volume: svh.OID,
			Aux:    aux,
		}); err != nil {
			return err
		}
		return nil
	})
}

func (c *Client) PutBranch(ctx context.Context, volh blobcache.Handle, bent BranchEntry) error {
	return c.doTx(ctx, volh, c.ActAs, func(tx *bcsdk.Tx, txb *Txn) error {
		return txb.PutBranch(ctx, bent)
	})
}

func (c *Client) DeleteBranch(ctx context.Context, volh blobcache.Handle, name string) error {
	return c.doTx(ctx, volh, c.ActAs, func(tx *bcsdk.Tx, txb *Txn) error {
		return txb.DeleteBranch(ctx, name)
	})
}

func (c *Client) ListBranches(ctx context.Context, volh blobcache.Handle, span branches.Span, limit int) ([]string, error) {
	volh, err := c.adjustHandle(ctx, volh)
	if err != nil {
		return nil, err
	}
	tx, err := bcsdk.BeginTx(ctx, c.Blobcache, volh, blobcache.TxParams{Mutate: false})
	if err != nil {
		return nil, err
	}
	defer tx.Abort(ctx)
	state, err := loadState(ctx, tx)
	if err != nil {
		return nil, err
	}
	entries, err := c.Machine.ListBranches(ctx, tx, *state, span, limit)
	if err != nil {
		return nil, err
	}
	names := slices2.Map(entries, func(e gotnsop.BranchEntry) string {
		return e.Name
	})
	return names, nil
}

func (c *Client) Inspect(ctx context.Context, volh blobcache.Handle, name string) (*branches.Info, error) {
	volh, err := c.adjustHandle(ctx, volh)
	if err != nil {
		return nil, err
	}
	tx, err := bcsdk.BeginTx(ctx, c.Blobcache, volh, blobcache.TxParams{Mutate: false})
	if err != nil {
		return nil, err
	}
	defer tx.Abort(ctx)
	return nil, nil
}

func (c *Client) OpenAt(ctx context.Context, nsh blobcache.Handle, name string) (branches.Volume, error) {
	nsh, err := c.adjustHandle(ctx, nsh)
	if err != nil {
		return nil, err
	}
	ent, err := c.GetBranch(ctx, nsh, name)
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

func (c *Client) adjustHandle(ctx context.Context, volh blobcache.Handle) (blobcache.Handle, error) {
	if volh.Secret == ([16]byte{}) {
		volh, err := c.Blobcache.OpenFiat(ctx, volh.OID, blobcache.Action_ALL)
		if err != nil {
			return blobcache.Handle{}, err
		}
		return *volh, nil
	} else {
		return volh, nil
	}
}

func (c *Client) doTx(ctx context.Context, volh blobcache.Handle, leafPriv LeafPrivate, fn func(tx *bcsdk.Tx, txn *Txn) error) error {
	if c.ActAs == (LeafPrivate{}) {
		return errors.New("gotns.Client: ActAs cannot be nil")
	}
	volh, err := c.adjustHandle(ctx, volh)
	if err != nil {
		return err
	}
	tx, err := bcsdk.BeginTx(ctx, c.Blobcache, volh, blobcache.TxParams{Mutate: true})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	var data []byte
	if err := tx.Load(ctx, &data); err != nil {
		return err
	}
	root, err := statetrace.Parse(data, ParseRoot)
	if err != nil {
		return err
	}
	txn := c.Machine.NewTxn(root, tx, []LeafPrivate{leafPriv})
	if err := fn(tx, txn); err != nil {
		return err
	}
	root2, err := txn.Finish(ctx)
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
	tx, err := bcsdk.BeginTx(ctx, c.Blobcache, volh, blobcache.TxParams{Mutate: false})
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

func (c *Client) createSubVolume(ctx context.Context, tx *bcsdk.Tx) (*blobcache.Handle, error) {
	volh, err := c.Blobcache.CreateVolume(ctx, nil, BranchVolumeSpec())
	if err != nil {
		return nil, err
	}
	if err := tx.Link(ctx, *volh, blobcache.Action_ALL); err != nil {
		return nil, err
	}
	return volh, nil
}

// IntroduceSelf creates a signed change set that adds a leaf to the state.
// Then it returns the signed change set data.
// It does not contact Blobcache or perform any Volume operations.
func (c *Client) IntroduceSelf(kemPub kem.PublicKey) gotnsop.ChangeSet {
	leaf := gotnsop.NewLeaf(c.ActAs.SigPrivateKey.Public().(inet256.PublicKey), kemPub)
	cs := gotnsop.ChangeSet{
		Ops: []Op{
			&gotnsop.CreateLeaf{
				Leaf: leaf,
			},
		},
	}
	cs.Sign(c.ActAs.SigPrivateKey)
	return cs
}

func loadState(ctx context.Context, tx *bcsdk.Tx) (*State, error) {
	var data []byte
	if err := tx.Load(ctx, &data); err != nil {
		return nil, err
	}
	root, err := statetrace.Parse(data, ParseRoot)
	if err != nil {
		return nil, err
	}
	return &root.State.Current, nil
}

// BranchVolumeSpec is the volume spec used for new branches.
func BranchVolumeSpec() blobcache.VolumeSpec {
	return blobcache.VolumeSpec{
		Local: &blobcache.VolumeBackend_Local{
			VolumeConfig: blobcache.VolumeConfig{
				Schema:   blobcache.SchemaSpec{Name: blobcache.Schema_NONE},
				HashAlgo: blobcache.HashAlgo_BLAKE2b_256,
				MaxSize:  1 << 22,
				Salted:   false,
			},
		},
	}
}
