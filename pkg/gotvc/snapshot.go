package gotvc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.brendoncarroll.net/tai64"

	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotfs"
)

type (
	Store = cadata.Store
	Ref   = gdat.Ref
	Root  = gotfs.Root
	Snap  = Snapshot
)

type Snapshot struct {
	N       uint64     `json:"n"`
	Root    gotfs.Root `json:"root"`
	Parents []gdat.Ref `json:"parents"`

	CreatedAt  tai64.TAI64 `json:"created_at"`
	Creator    string      `json:"creator,omitempty"`
	AuthoredAt tai64.TAI64 `json:"authored_at"`
	Authors    []string    `json:"authors,omitempty"`

	Message string `json:"message"`
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
		gotfs.Equal(a.Root, b.Root) &&
		parentsEqual
}

type SnapInfo struct {
	CreatedAt  tai64.TAI64
	Creator    string
	AuthoredAt tai64.TAI64
	Authors    []string

	Message string
}

func (a *Agent) NewSnapshot(ctx context.Context, s cadata.Store, parents []Snapshot, root Root, sinfo SnapInfo) (*Snapshot, error) {
	var n uint64
	parentRefs := make([]Ref, len(parents))
	for i, parent := range parents {
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
		N:       n,
		Root:    root,
		Parents: parentRefs,

		CreatedAt:  sinfo.CreatedAt,
		Creator:    sinfo.Creator,
		AuthoredAt: sinfo.AuthoredAt,
		Authors:    sinfo.Authors,

		Message: sinfo.Message,
	}, nil
}

// NewZero creates a new snapshot with no parent
func (ag *Agent) NewZero(ctx context.Context, s cadata.Store, root Root, sinfo SnapInfo) (*Snapshot, error) {
	return ag.NewSnapshot(ctx, s, nil, root, sinfo)
}

// PostSnapshot marshals the snapshot and posts it to the store
func (ag *Agent) PostSnapshot(ctx context.Context, s Store, x Snapshot) (*Ref, error) {
	if ag.readOnly {
		panic("gotvc: operator is read-only. This is a bug.")
	}
	return ag.da.Post(ctx, s, marshalSnapshot(x))
}

// GetSnapshot retrieves the snapshot referenced by ref from the store.
func (ag *Agent) GetSnapshot(ctx context.Context, s Store, ref Ref) (*Snapshot, error) {
	var x *Snapshot
	if err := ag.da.GetF(ctx, s, ref, func(data []byte) error {
		var err error
		x, err = parseSnapshot(data)
		return err
	}); err != nil {
		return nil, err
	}
	return x, nil
}

// Squash turns multiple snapshots into one.
// It preserves the latest version of the data, but destroys versioning granularity
func (ag *Agent) Squash(ctx context.Context, s Store, x Snapshot, n int) (*Snapshot, error) {
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
			Root:    x.Root,
			Parents: parent.Parents,
		}, nil
	}
	y, err := ag.Squash(ctx, s, *parent, n-1)
	if err != nil {
		return nil, err
	}
	y.Root = x.Root
	return y, nil
}

func marshalSnapshot(x Snapshot) []byte {
	data, err := json.Marshal(x)
	if err != nil {
		panic(err)
	}
	return data
}

func parseSnapshot(data []byte) (*Snapshot, error) {
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}

// RefFromSnapshot computes a ref for snap if it was posted to s.
// It only calls s.Hash and s.MaxSize; it does not mutate s.
func (ag *Agent) RefFromSnapshot(snap Snapshot, s cadata.Store) Ref {
	s2 := cadata.NewVoid(s.Hash, s.MaxSize())
	ref, err := ag.PostSnapshot(context.TODO(), s2, snap)
	if err != nil {
		panic(err)
	}
	return *ref
}

// Check ensures that snapshot is valid.
func (a *Agent) Check(ctx context.Context, s cadata.Store, snap Snapshot, checkRoot func(gotfs.Root) error) error {
	logctx.Infof(ctx, "checking snapshot #%d", snap.N)
	if err := checkRoot(snap.Root); err != nil {
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
