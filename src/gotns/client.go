package gotns

import (
	"context"
	"errors"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/volumes"
	"go.brendoncarroll.net/exp/slices2"
	"go.inet256.org/inet256/src/inet256"
)

type Client struct {
	Blobcache blobcache.Service
	Machine   Machine
	ActAs     inet256.PrivateKey
}

// Init initializes a new GotNS instance in the given volume.
func (c *Client) Init(ctx context.Context, volh blobcache.Handle) error {
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
	root, err := c.Machine.New(ctx, tx)
	if err != nil {
		return err
	}
	return tx.Commit(ctx, root.Marshal(nil))
}

func (c *Client) GetEntry(ctx context.Context, volh blobcache.Handle, name string) (*Entry, error) {
	volh, err := c.adjustHandle(ctx, volh)
	if err != nil {
		return nil, err
	}
	tx, err := blobcache.BeginTx(ctx, c.Blobcache, volh, blobcache.TxParams{Mutate: true})
	if err != nil {
		return nil, err
	}
	defer tx.Abort(ctx)
	root, err := loadRoot(ctx, tx)
	if err != nil {
		return nil, err
	}
	return c.Machine.GetEntry(ctx, tx, *root, []byte(name))
}

func (c *Client) PutEntry(ctx context.Context, volh blobcache.Handle, name string, entry branches.Info) error {
	panic("not implemented")
}

func (c *Client) DeleteEntry(ctx context.Context, volh blobcache.Handle, name string) error {
	return c.doTx(ctx, volh, func(s stores.RW, root *Root) (*Root, error) {
		return c.Machine.DeleteEntry(ctx, s, *root, name)
	})
}

func (c *Client) ListEntries(ctx context.Context, volh blobcache.Handle, limit int) ([]string, error) {
	volh, err := c.adjustHandle(ctx, volh)
	if err != nil {
		return nil, err
	}
	tx, err := blobcache.BeginTx(ctx, c.Blobcache, volh, blobcache.TxParams{Mutate: true})
	if err != nil {
		return nil, err
	}
	defer tx.Abort(ctx)
	root, err := loadRoot(ctx, tx)
	if err != nil {
		return nil, err
	}
	entries, err := c.Machine.ListEntries(ctx, tx, *root, limit)
	if err != nil {
		return nil, err
	}
	return slices2.Map(entries, func(e Entry) string {
		return e.Name
	}), nil
}

func (c *Client) Inspect(ctx context.Context, volh blobcache.Handle, name string) (*branches.Info, error) {
	volh, err := c.adjustHandle(ctx, volh)
	if err != nil {
		return nil, err
	}
	tx, err := blobcache.BeginTx(ctx, c.Blobcache, volh, blobcache.TxParams{Mutate: true})
	if err != nil {
		return nil, err
	}
	defer tx.Abort(ctx)
	return nil, nil
}

func (c *Client) CreateAt(ctx context.Context, nsh blobcache.Handle, name string, binfo branches.Info) error {
	nsh, err := c.adjustHandle(ctx, nsh)
	if err != nil {
		return err
	}
	tx, err := blobcache.BeginTx(ctx, c.Blobcache, nsh, blobcache.TxParams{Mutate: true})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)

	spec := blobcache.DefaultLocalSpec()
	spec.Local.HashAlgo = blobcache.HashAlgo_BLAKE2b_256
	volh, err := c.Blobcache.CreateVolume(ctx, nil, spec)
	if err != nil {
		return err
	}
	if err := tx.AllowLink(ctx, *volh); err != nil {
		return err
	}
	root, err := loadRoot(ctx, tx)
	if err != nil {
		return err
	}
	root, err = c.Machine.PutEntry(ctx, tx, *root, Entry{
		Name:   name,
		Volume: volh.OID,
		Rights: blobcache.Action_ALL,
	})
	if err != nil {
		return err
	}
	return tx.Commit(ctx, root.Marshal(nil))
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
	return &volumes.Blobcache{
		Handle:  *volh,
		Service: c.Blobcache,
	}, nil
}

// AddLeaf adds a new primitive identity to a group.
func (c *Client) AddLeaf(ctx context.Context, volh blobcache.Handle, leaf IdentityLeaf) error {
	return nil
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
	root, err := loadRoot(ctx, tx)
	if err != nil {
		return err
	}
	root, err = c.Machine.AddMember(ctx, tx, *root, name, member)
	if err != nil {
		return err
	}
	return tx.Commit(ctx, root.Marshal(nil))
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

func (c *Client) doTx(ctx context.Context, volh blobcache.Handle, fn func(s stores.RW, root *Root) (*Root, error)) error {
	volh, err := c.adjustHandle(ctx, volh)
	if err != nil {
		return err
	}
	tx, err := blobcache.BeginTx(ctx, c.Blobcache, volh, blobcache.TxParams{Mutate: true})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	root, err := loadRoot(ctx, tx)
	if err != nil {
		return err
	}
	root, err = fn(tx, root)
	if err != nil {
		return err
	}
	return tx.Commit(ctx, root.Marshal(nil))
}

func loadRoot(ctx context.Context, tx *blobcache.Tx) (*Root, error) {
	var data []byte
	if err := tx.Load(ctx, &data); err != nil {
		return nil, err
	}
	var r Root
	if err := r.Unmarshal(data); err != nil {
		return nil, err
	}
	return &r, nil
}
