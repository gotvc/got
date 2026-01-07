package gotorg

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"blobcache.io/blobcache/src/schema/statetrace"
	"github.com/gotvc/got/src/gotorg/internal/gotorgop"
	"github.com/gotvc/got/src/internal/stores"
	"go.inet256.org/inet256/src/inet256"
)

type Op = gotorgop.Op

// Txn allows a transction to be built up incrementally
// And then turned into a single slot change to the ledger.
type Txn struct {
	m     *Machine
	prev  statetrace.Root[Root]
	s     schema.RW
	actAs []IdenPrivate

	curState State
	changes  []Op
}

// NewBuilder creates a new delta builder.
// privateKey is the private key of the actor performing the transaction.
// It will be used to produce a signature at the end of the transaction.
func (m *Machine) NewTxn(prev statetrace.Root[Root], s schema.RW, actAs []IdenPrivate) *Txn {
	return &Txn{
		m:     m,
		prev:  prev,
		s:     s,
		actAs: actAs,

		curState: prev.State.Current,
		changes:  []Op{},
	}
}

func (tx *Txn) addOp(op Op) {
	tx.changes = append(tx.changes, op)
}

// Finish applies the changes to the previous root, and returns the new root.
func (tx *Txn) Finish(ctx context.Context) (statetrace.Root[Root], error) {
	cs := gotorgop.ChangeSet{
		Ops: tx.changes,
	}
	for _, signer := range tx.actAs {
		cs.Sign(signer.SigPrivateKey)
	}

	s2 := stores.AddWriteLayer(tx.s, stores.NewMem())
	if err := tx.m.ValidateChange(ctx, s2, tx.prev.State.Current, tx.curState, Delta(cs)); err != nil {
		return statetrace.Root[Root]{}, err
	}
	nextRoot := Root{
		Current: tx.curState,
		Recent:  Delta(cs),
	}
	return tx.m.led.AndThen(ctx, tx.s, tx.prev, nextRoot)
}

// CreateIDUnit creates a new unit.
func (tx *Txn) CreateIDUnit(ctx context.Context, unit IdentityUnit) error {
	state, err := tx.m.PutIDUnit(ctx, tx.s, tx.curState, unit)
	if err != nil {
		return err
	}
	tx.curState = *state
	tx.addOp(&gotorgop.CreateIDUnit{
		Unit: unit,
	})
	return nil
}

func (tx *Txn) AddMember(ctx context.Context, gid gotorgop.GroupID, member gotorgop.Member) error {
	priv := tx.actAs[0]
	groupPath, err := tx.m.FindGroupPath(ctx, tx.s, tx.curState, priv.GetID(), gid)
	if err != nil {
		return err
	}
	groupSecret, err := tx.m.GetGroupSecret(ctx, tx.s, tx.curState, priv, groupPath)
	if err != nil {
		return err
	}
	nextState, err := tx.m.AddMember(ctx, tx.s, tx.curState, gid, member, groupSecret)
	if err != nil {
		return err
	}
	tx.curState = *nextState
	tx.addOp(&gotorgop.AddMember{
		Group:  gid,
		Member: member,
	})
	return nil
}

func (tx *Txn) LookupGroup(ctx context.Context, gname string) (*Group, error) {
	return tx.m.LookupGroup(ctx, tx.s, tx.curState, gname)
}

func (tx *Txn) PutAlias(ctx context.Context, entry VolumeAlias, secret *gotorgop.Secret) error {
	state, err := tx.m.PutAlias(ctx, tx.s, tx.curState, entry, secret)
	if err != nil {
		return err
	}
	tx.curState = *state
	tx.addOp(&gotorgop.PutBranchEntry{
		Name:   entry.Name,
		Volume: entry.Volume,
	})
	return nil
}

func (tx *Txn) DeleteAlias(ctx context.Context, name string) error {
	state, err := tx.m.DeleteAlias(ctx, tx.s, tx.curState, name)
	if err != nil {
		return err
	}
	tx.curState = *state
	tx.addOp(&gotorgop.DeleteBranchEntry{
		Name: name,
	})
	return nil
}

func (tx *Txn) AddVolume(ctx context.Context, vent gotorgop.VolumeEntry) error {
	state, err := tx.m.AddVolume(ctx, tx.s, tx.curState, vent)
	if err != nil {
		return err
	}
	tx.curState = *state
	tx.addOp(&gotorgop.AddVolume{Volume: vent.Target})
	return nil
}

func (tx *Txn) DropVolume(ctx context.Context, volid blobcache.OID) error {
	state, err := tx.m.DropVolume(ctx, tx.s, tx.curState, volid)
	if err != nil {
		return err
	}
	tx.curState = *state
	tx.addOp(&gotorgop.DropVolume{Volume: volid})
	return nil
}

func (tx *Txn) ChangeSet(ctx context.Context, cs gotorgop.ChangeSet) error {
	for _, op := range cs.Ops {
		// TODO: this is not great, we should only implement this once in CreateLeaf.
		switch op := op.(type) {
		case *gotorgop.CreateIDUnit:
			if err := tx.createLeaf(ctx, op.Unit); err != nil {
				return err
			}

		default:
			return fmt.Errorf("cannot apply op in change set: %T", op)
		}
	}
	tx.addOp(&cs)
	return nil
}

func (tx *Txn) createLeaf(ctx context.Context, leaf IdentityUnit) error {
	state, err := tx.m.PutIDUnit(ctx, tx.s, tx.curState, leaf)
	if err != nil {
		return err
	}
	tx.curState = *state
	return nil
}

type IDSet = map[inet256.ID]struct{}
