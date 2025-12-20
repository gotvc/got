package gotvc

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"go.brendoncarroll.net/stdctx/logctx"
	"go.brendoncarroll.net/tai64"
	"go.inet256.org/inet256/src/inet256"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/internal/sbe"
	"github.com/gotvc/got/src/internal/stores"
)

type (
	Ref  = gdat.Ref
	Root = gotfs.Root
	Snap = Snapshot
)

// Payload is the thing being snapshotted.
type Payload struct {
	Root gotfs.Root
	Aux  []byte
}

func (p Payload) Marshal(out []byte) []byte {
	out = p.Root.Marshal(out)
	out = sbe.AppendLP(out, p.Aux)
	return out
}

func (p *Payload) Unmarshal(data []byte) error {
	rootData, data, err := sbe.ReadN(data, gotfs.RootSize)
	if err != nil {
		return err
	}
	root, err := gotfs.ParseRoot(rootData)
	if err != nil {
		return err
	}
	p.Root = *root
	auxData, _, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	p.Aux = auxData
	return nil
}

type Snapshot struct {
	// N is the critical distance to the root.
	// N is 0 if there are no parents.
	// N is the max of the parents' N + 1.
	N uint64
	// CreatedAt is the time the snapshot was created.
	CreatedAt tai64.TAI64
	Parents   []gdat.Ref
	// Creator is the ID of the user who created the snapshot.
	Creator inet256.ID

	// Payload is the thing being snapshotted.
	Payload Payload
}

func ParseSnapshot(data []byte) (*Snapshot, error) {
	var a Snapshot
	if err := a.Unmarshal(data); err != nil {
		return nil, err
	}
	return &a, nil
}

func (a Snapshot) Marshal(out []byte) []byte {
	out = sbe.AppendUint64(out, a.N)
	out = append(out, a.CreatedAt.Marshal()...)

	// parents
	if len(a.Parents) > 65535 {
		panic(fmt.Errorf("too many parents: %d", len(a.Parents)))
	}
	out = sbe.AppendUint16(out, uint16(len(a.Parents)))
	for _, parent := range a.Parents {
		out = gdat.AppendRef(out, parent)
	}

	out = append(out, a.Creator[:]...)

	out = sbe.AppendLP(out, a.Payload.Marshal(nil))
	return out
}

func (a *Snapshot) Unmarshal(data []byte) error {
	// N
	n, data, err := sbe.ReadUint64(data)
	if err != nil {
		return err
	}
	a.N = n
	// createdAt
	createdAtData, data, err := sbe.ReadN(data, 8)
	if err != nil {
		return err
	}
	createdAt, err := tai64.Parse(createdAtData)
	if err != nil {
		return err
	}
	a.CreatedAt = createdAt
	// parents
	numParents, data, err := sbe.ReadUint16(data)
	if err != nil {
		return err
	}
	a.Parents = make([]gdat.Ref, numParents)
	for i := range a.Parents {
		refData, rest, err := sbe.ReadN(data, gdat.RefSize)
		if err != nil {
			return err
		}
		ref, err := gdat.ParseRef(refData)
		if err != nil {
			return err
		}
		a.Parents[i] = ref
		data = rest
	}

	// creator
	creatorData, data, err := sbe.ReadN(data, inet256.AddrSize)
	if err != nil {
		return err
	}
	a.Creator = inet256.ID(creatorData)

	// payload
	payloadData, _, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	if err := a.Payload.Unmarshal(payloadData); err != nil {
		return err
	}
	return nil
}

func (a Snapshot) Equals(b Snapshot) bool {
	var parentsEqual bool
	if len(a.Parents) != len(b.Parents) {
		parentsEqual = false
	} else {
		parentsEqual = true
		for i := range a.Parents {
			parentsEqual = gdat.Equal(a.Parents[i], b.Parents[i])
			if !parentsEqual {
				break
			}
		}
	}
	return a.N == b.N &&
		gotfs.Equal(a.Payload.Root, b.Payload.Root) &&
		parentsEqual
}

// SnapParams are the parameters required to create a new snapshot.
type SnapParams struct {
	Parents   []Snapshot
	Creator   inet256.ID
	CreatedAt tai64.TAI64
}

func (a *Machine) NewSnapshot(ctx context.Context, s stores.Writing, sp SnapParams, payload Payload) (*Snapshot, error) {
	var n uint64
	parentRefs := make([]Ref, len(sp.Parents))
	for i, parent := range sp.Parents {
		parentRef, err := a.PostSnapshot(ctx, s, parent)
		if err != nil {
			return nil, err
		}
		if n < parent.N+1 {
			n = parent.N + 1
		}
		parentRefs[i] = *parentRef
	}
	sort.Slice(parentRefs, func(i, j int) bool {
		a, b := parentRefs[i].CID, parentRefs[j].CID
		return a.Compare(b) < 0
	})
	return &Snapshot{
		N:         n,
		CreatedAt: sp.CreatedAt,
		Parents:   parentRefs,
		Creator:   sp.Creator,

		Payload: payload,
	}, nil
}

// NewZero creates a new snapshot with no parent
func (mach *Machine) NewZero(ctx context.Context, s stores.Writing, sp SnapParams, payload Payload) (*Snapshot, error) {
	sp.Parents = nil
	return mach.NewSnapshot(ctx, s, sp, payload)
}

// PostSnapshot marshals the snapshot and posts it to the store
func (ag *Machine) PostSnapshot(ctx context.Context, s stores.Writing, x Snapshot) (*Ref, error) {
	if ag.readOnly {
		panic("gotvc: operator is read-only. This is a bug.")
	}
	return ag.da.Post(ctx, s, x.Marshal(nil))
}

// GetSnapshot retrieves the snapshot referenced by ref from the store.
func (ag *Machine) GetSnapshot(ctx context.Context, s stores.Reading, ref Ref) (*Snapshot, error) {
	var x *Snapshot
	if err := ag.da.GetF(ctx, s, ref, func(data []byte) error {
		var err error
		x, err = ParseSnapshot(data)
		return err
	}); err != nil {
		return nil, err
	}
	return x, nil
}

// Squash turns multiple snapshots into one.
// It preserves the latest version of the data, but destroys versioning granularity
func (ag *Machine) Squash(ctx context.Context, s stores.RW, x Snapshot, n int) (*Snapshot, error) {
	if n < 1 {
		return nil, fmt.Errorf("cannot squash single commit")
	}
	if len(x.Parents) < 1 {
		return nil, fmt.Errorf("cannot squash no parent")
	}
	if len(x.Parents) > 1 {
		return nil, fmt.Errorf("cannot rebase > 1 parents")
	}
	parent, err := ag.GetSnapshot(ctx, s, x.Parents[0])
	if err != nil {
		return nil, err
	}
	if n == 1 {
		return &Snapshot{
			N:       parent.N,
			Payload: x.Payload,
			Parents: parent.Parents,
		}, nil
	}
	y, err := ag.Squash(ctx, s, *parent, n-1)
	if err != nil {
		return nil, err
	}
	y.Payload = x.Payload
	return y, nil
}

// RefFromSnapshot computes a ref for snap if it was posted to s.
// It only calls s.Hash and s.MaxSize; it does not mutate s.
func (ag *Machine) RefFromSnapshot(snap Snapshot) Ref {
	s2 := stores.NewVoid()
	ref, err := ag.PostSnapshot(context.TODO(), s2, snap)
	if err != nil {
		panic(err)
	}
	return *ref
}

// Check ensures that snapshot is valid.
func (a *Machine) Check(ctx context.Context, s stores.Reading, snap Snapshot, checkRoot func(Payload) error) error {
	logctx.Infof(ctx, "checking snapshot #%d", snap.N)
	if err := checkRoot(snap.Payload); err != nil {
		return err
	}
	if len(snap.Parents) == 0 {
		return nil
	}
	for i := 0; i < len(snap.Parents)-1; i++ {
		if bytes.Compare(snap.Parents[i].CID[:], snap.Parents[i+1].CID[:]) < 0 {
			return fmt.Errorf("unsorted parents")
		}
	}
	for _, parentRef := range snap.Parents {
		parent, err := a.GetSnapshot(ctx, s, parentRef)
		if err != nil {
			return err
		}
		if err := a.Check(ctx, s, *parent, checkRoot); err != nil {
			return err
		}
	}
	return nil
}
