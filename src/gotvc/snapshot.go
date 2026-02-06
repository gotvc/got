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
	"github.com/gotvc/got/src/internal/sbe"
	"github.com/gotvc/got/src/internal/stores"
)

type Marshalable interface {
	Marshal(out []byte) []byte
}

type Vertex[T Marshalable] struct {
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
	Payload T
}

func ParseVertex[T Marshalable](data []byte, parser Parser[T]) (*Vertex[T], error) {
	var a Vertex[T]
	if err := a.Unmarshal(data, parser); err != nil {
		return nil, err
	}
	return &a, nil
}

func (a Vertex[T]) Marshal(out []byte) []byte {
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

func (a *Vertex[T]) Unmarshal(data []byte, parsePayload Parser[T]) error {
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
	payload, err := parsePayload(payloadData)
	if err != nil {
		return err
	}
	a.Payload = payload

	return nil
}

func (a Vertex[T]) Equals(b Vertex[T]) bool {
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
		bytes.Equal(a.Payload.Marshal(nil), b.Payload.Marshal(nil)) &&
		parentsEqual
}

// SnapshotParams are the parameters required to create a new snapshot.
type SnapshotParams[T Marshalable] struct {
	Parents   []Vertex[T]
	Creator   inet256.ID
	CreatedAt tai64.TAI64
	// Payload is the thing being snapshot
	Payload T
}

func (a *Machine[T]) NewSnapshot(ctx context.Context, s stores.Writing, sp SnapshotParams[T]) (*Vertex[T], error) {
	var n uint64
	maxCreatedAt := sp.CreatedAt
	parentRefs := make([]Ref, len(sp.Parents))
	for i, parent := range sp.Parents {
		maxCreatedAt = max(maxCreatedAt, parent.CreatedAt)
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
	return &Vertex[T]{
		N:         n,
		CreatedAt: maxCreatedAt,
		Parents:   parentRefs,
		Creator:   sp.Creator,
		Payload:   sp.Payload,
	}, nil
}

// NewZero creates a new snapshot with no parent
func (mach *Machine[T]) NewZero(ctx context.Context, s stores.Writing, sp SnapshotParams[T]) (*Vertex[T], error) {
	sp.Parents = nil
	return mach.NewSnapshot(ctx, s, sp)
}

// PostSnapshot marshals the snapshot and posts it to the store
func (ag *Machine[T]) PostSnapshot(ctx context.Context, s stores.Writing, x Vertex[T]) (*Ref, error) {
	if ag.readOnly {
		panic("gotvc: operator is read-only. This is a bug.")
	}
	return ag.da.Post(ctx, s, x.Marshal(nil))
}

// GetSnapshot retrieves the snapshot referenced by ref from the store.
func (ag *Machine[T]) GetSnapshot(ctx context.Context, s stores.Reading, ref Ref) (*Vertex[T], error) {
	var x *Vertex[T]
	if err := ag.da.GetF(ctx, s, ref, func(data []byte) error {
		var err error
		x, err = ParseVertex[T](data, ag.parse)
		return err
	}); err != nil {
		return nil, err
	}
	return x, nil
}

// Squash turns multiple snapshots into one.
// It preserves the latest version of the data, but destroys versioning granularity
func (ag *Machine[T]) Squash(ctx context.Context, s stores.RW, x Vertex[T], n int) (*Vertex[T], error) {
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
		return &Vertex[T]{
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
func (ag *Machine[T]) RefFromSnapshot(snap Vertex[T]) Ref {
	s2 := stores.NewVoid()
	ref, err := ag.PostSnapshot(context.TODO(), s2, snap)
	if err != nil {
		panic(err)
	}
	return *ref
}

// Check ensures that snapshot is valid.
func (a *Machine[T]) Check(ctx context.Context, s stores.Reading, snap Vertex[T], checkRoot func(T) error) error {
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
