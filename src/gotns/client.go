package gotns

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/statetrace"
	"github.com/cloudflare/circl/kem"
	"github.com/cloudflare/circl/sign"
	"github.com/gotvc/got/src/branches"
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
func (c *Client) EnsureInit(ctx context.Context, volh blobcache.Handle, admins []IdentityUnit) error {
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

func (c *Client) LookupGroup(ctx context.Context, volh blobcache.Handle, name string) (*gotnsop.Group, error) {
	var group *gotnsop.Group
	if err := c.view(ctx, volh, func(s stores.Reading, state State) error {
		var err error
		group, err = c.Machine.LookupGroup(ctx, s, state, name)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return group, nil
}

func (c *Client) GetIDUnit(ctx context.Context, volh blobcache.Handle, id inet256.ID) (*IdentityUnit, error) {
	var idu *IdentityUnit
	if err := c.view(ctx, volh, func(s stores.Reading, state State) error {
		var err error
		idu, err = c.Machine.GetIDUnit(ctx, s, state, id)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return idu, nil
}

func (c *Client) GetAlias(ctx context.Context, volh blobcache.Handle, name string) (*AliasEntry, error) {
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
	return c.Machine.GetAlias(ctx, tx, *state, name)
}

// CreateAlias creates a new alias with a new volume at the specified name.
func (c *Client) CreateAlias(ctx context.Context, nsh blobcache.Handle, name string, aux []byte) error {
	return c.doTx(ctx, nsh, c.ActAs, func(tx *bcsdk.Tx, txn *Txn) error {
		svh, err := c.createSubVolume(ctx, tx)
		if err != nil {
			return err
		}
		if err := tx.Link(ctx, *svh, blobcache.Action_ALL); err != nil {
			return err
		}
		sec := gotnsop.Secret{}
		if _, err := rand.Read(sec[:]); err != nil {
			return err
		}
		hos := [32]byte(sec.Ratchet(2))
		if err := txn.AddVolume(ctx, VolumeEntry{
			Volume:        svh.OID,
			HashOfSecrets: [][32]byte{hos},
		}); err != nil {
			return err
		}
		if err := txn.PutAlias(ctx, gotnsop.AliasEntry{
			Name:   name,
			Volume: svh.OID,
		}, &sec); err != nil {
			return err
		}
		return nil
	})
}

func (c *Client) PutAlias(ctx context.Context, volh blobcache.Handle, bent AliasEntry, secret *gotnsop.Secret) error {
	return c.doTx(ctx, volh, c.ActAs, func(tx *bcsdk.Tx, txb *Txn) error {
		return txb.PutAlias(ctx, bent, secret)
	})
}

func (c *Client) DeleteAlias(ctx context.Context, volh blobcache.Handle, name string) error {
	return c.doTx(ctx, volh, c.ActAs, func(tx *bcsdk.Tx, txb *Txn) error {
		return txb.DeleteAlias(ctx, name)
	})
}

func (c *Client) ListAliases(ctx context.Context, volh blobcache.Handle, span branches.Span, limit int) ([]string, error) {
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
	names := slices2.Map(entries, func(e gotnsop.AliasEntry) string {
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

func (c *Client) OpenAt(ctx context.Context, nsh blobcache.Handle, name string, actAs LeafPrivate) (branches.Volume, error) {
	var vent *VolumeEntry
	var secret *gotnsop.Secret
	nsh, err := c.adjustHandle(ctx, nsh)
	if err != nil {
		return nil, err
	}
	if err := c.view(ctx, nsh, func(s stores.Reading, state State) error {
		ent, err := c.Machine.GetAlias(ctx, s, state, name)
		if err != nil {
			return err
		}
		vent, err = c.Machine.GetVolume(ctx, s, state, ent.Volume)
		if err != nil {
			return err
		}
		secret, err = c.Machine.FindSecret(ctx, s, state, actAs, &vent.HashOfSecrets[0])
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, fmt.Errorf("secret not found")
	}
	volh, err := c.Blobcache.OpenFrom(ctx, nsh, vent.Volume, blobcache.Action_ALL)
	if err != nil {
		return nil, err
	}
	innerVol := &volumes.Blobcache{
		Handle:  *volh,
		Service: c.Blobcache,
	}

	readSecret := secret.DeriveSym()
	vol := newVolume(innerVol, &readSecret, actAs.SigPrivateKey, func(ctx context.Context, id inet256.ID) (sign.PublicKey, error) {
		var verifier sign.PublicKey
		if err := c.view(ctx, nsh, func(s stores.Reading, x State) error {
			idUnit, err := c.Machine.GetIDUnit(ctx, s, x, id)
			if err != nil {
				return err
			}
			if idUnit == nil {
				return fmt.Errorf("info for id %v not found", id)
			}
			yes, err := c.Machine.CanDo(ctx, s, x, id, gotnsop.Verb_TOUCH, gotnsop.ObjectType_BRANCH, name)
			if err != nil {
				return err
			}
			if !yes {
				return fmt.Errorf("id %v is not allowed to write to this branch", id)
			}
			verifier = idUnit.SigPublicKey
			return nil
		}); err != nil {
			return nil, err
		}
		return verifier, nil
	})
	return vol, nil
}

// AddLeaf adds a new primitive identity to a group.
func (c *Client) AddIDUnit(ctx context.Context, volh blobcache.Handle, idu IdentityUnit) error {
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
	leaf := gotnsop.NewIDUnit(c.ActAs.SigPrivateKey.Public().(inet256.PublicKey), kemPub)
	cs := gotnsop.ChangeSet{
		Ops: []Op{
			&gotnsop.CreateIDUnit{
				Unit: leaf,
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
