package gotns

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/schema"
	"blobcache.io/blobcache/src/schema/statetrace"
	"github.com/gotvc/got/src/gotns/internal/gotnsop"
	"github.com/gotvc/got/src/internal/stores"
	"go.inet256.org/inet256/src/inet256"
)

type Op = gotnsop.Op

// Txn allows a transction to be built up incrementally
// And then turned into a single slot change to the ledger.
type Txn struct {
	m     *Machine
	prev  statetrace.Root[Root]
	s     schema.RW
	actAs []LeafPrivate

	curState State
	changes  []Op
}

// NewBuilder creates a new delta builder.
// privateKey is the private key of the actor performing the transaction.
// It will be used to produce a signature at the end of the transaction.
func (m *Machine) NewTxn(prev statetrace.Root[Root], s schema.RW, actAs []LeafPrivate) *Txn {
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
	cs := gotnsop.ChangeSet{
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

// CreateLeaf creates a new leaf.
func (tx *Txn) CreateLeaf(ctx context.Context, leaf IdentityLeaf) error {
	if err := tx.createLeaf(ctx, leaf); err != nil {
		return err
	}
	tx.addOp(&gotnsop.CreateLeaf{
		Leaf: leaf,
	})
	return nil
}

// AddLeaf adds a leaf in a transaction.
func (tx *Txn) AddLeaf(ctx context.Context, group string, leafID inet256.ID) error {
	if len(tx.actAs) > 1 {
		return fmt.Errorf("cannot add leaf in a transaction with multiple signers")
	}
	actAs := tx.actAs[0]
	ownerID := pki.NewID(actAs.SigPrivateKey.Public().(inet256.PublicKey))
	kemSeed, err := tx.m.GetKEMSeed(ctx, tx.s, tx.curState, []string{group}, ownerID, actAs.KEMPrivateKey)
	if err != nil {
		return err
	}
	nextState, err := tx.m.AddGroupLeaf(ctx, tx.s, tx.curState, kemSeed, group, leafID)
	if err != nil {
		return err
	}
	tx.curState = *nextState
	tx.addOp(&gotnsop.AddMember{
		Group:  group,
		Member: leafID.String(),
	})
	return nil
}

func (tx *Txn) PutEntry(ctx context.Context, entry Entry) error {
	state, err := tx.m.PutEntry(ctx, tx.s, tx.curState, entry)
	if err != nil {
		return err
	}
	tx.curState = *state
	tx.addOp(&gotnsop.PutEntry{
		Entry: entry,
	})
	return nil
}

func (tx *Txn) DeleteEntry(ctx context.Context, name string) error {
	state, err := tx.m.DeleteEntry(ctx, tx.s, tx.curState, name)
	if err != nil {
		return err
	}
	tx.curState = *state
	tx.addOp(&gotnsop.DeleteEntry{
		Name: name,
	})
	return nil
}

func (tx *Txn) ChangeSet(ctx context.Context, cs gotnsop.ChangeSet) error {
	for _, op := range cs.Ops {
		// TODO: this is not great, we should only implement this once in CreateLeaf.
		switch op := op.(type) {
		case *gotnsop.CreateLeaf:
			if err := tx.createLeaf(ctx, op.Leaf); err != nil {
				return err
			}

		default:
			return fmt.Errorf("cannot apply op in change set: %T", op)
		}
	}
	tx.addOp(&cs)
	return nil
}

func (tx *Txn) createLeaf(ctx context.Context, leaf IdentityLeaf) error {
	state, err := tx.m.PutLeaf(ctx, tx.s, tx.curState, leaf)
	if err != nil {
		return err
	}
	tx.curState = *state
	return nil
}

type IDSet = map[inet256.ID]struct{}
