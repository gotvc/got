package gotvc

import (
	"context"
	"encoding/json"
	"time"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type (
	Store = cadata.Store
	Ref   = gdat.Ref
	Root  = gotfs.Root
	Snap  = Snapshot
)

type Snapshot struct {
	N      uint64     `json:"n"`
	Root   gotfs.Root `json:"root"`
	Parent *gdat.Ref  `json:"parent"`

	Message   string     `json:"message,omitempty"`
	CreatedAt *time.Time `json:"created_at,omitempty"`
	Creator   p2p.PeerID `json:"creator,omitempty"`
}

func (a Snapshot) Equals(b Snapshot) bool {
	var parentsEqual bool
	switch {
	case a.Parent == nil && b.Parent == nil:
		parentsEqual = true
	case a.Parent != nil && b.Parent != nil:
		parentsEqual = gdat.Equal(*a.Parent, *b.Parent)
	}
	return a.N == b.N &&
		gotfs.Equal(a.Root, b.Root) &&
		parentsEqual
}

type SnapInfo struct {
	Message   string
	CreatedAt *time.Time
}

func (o *Operator) NewSnapshot(ctx context.Context, s cadata.Store, parent *Snapshot, root Root, sinfo SnapInfo) (*Snapshot, error) {
	var parentRef *Ref
	var n uint64
	if parent != nil {
		var err error
		parentRef, err = o.PostSnapshot(ctx, s, *parent)
		if err != nil {
			return nil, err
		}
		n = parent.N + 1
	}
	return &Snapshot{
		N:      n,
		Root:   root,
		Parent: parentRef,

		Message:   sinfo.Message,
		CreatedAt: sinfo.CreatedAt,
	}, nil
}

// NewZero creates a new snapshot with no parent
func (op *Operator) NewZero(ctx context.Context, s cadata.Store, root Root, sinfo SnapInfo) (*Snapshot, error) {
	return op.NewSnapshot(ctx, s, nil, root, sinfo)
}

// PostSnapshot marshals the snapshot and posts it to the store
func (op *Operator) PostSnapshot(ctx context.Context, s Store, x Snapshot) (*Ref, error) {
	if op.readOnly {
		panic("gotvc: operator is read-only. This is a bug.")
	}
	return op.dop.Post(ctx, s, marshalSnapshot(x))
}

// GetSnapshot retrieves the snapshot referenced by ref from the store.
func (op *Operator) GetSnapshot(ctx context.Context, s Store, ref Ref) (*Snapshot, error) {
	var x *Snapshot
	if err := op.dop.GetF(ctx, s, ref, func(data []byte) error {
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
func (op *Operator) Squash(ctx context.Context, s Store, x Snapshot, n int) (*Snapshot, error) {
	if n < 1 {
		return nil, errors.Errorf("cannot squash single commit")
	}
	if x.Parent == nil {
		return nil, errors.Errorf("cannot squash no parent")
	}
	parent, err := op.GetSnapshot(ctx, s, *x.Parent)
	if err != nil {
		return nil, err
	}
	if n == 1 {
		return &Snapshot{
			N:      parent.N,
			Root:   x.Root,
			Parent: parent.Parent,
		}, nil
	}
	y, err := op.Squash(ctx, s, *parent, n-1)
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

func (op *Operator) RefFromSnapshot(snap Snapshot) Ref {
	ref, err := op.PostSnapshot(context.Background(), cadata.Void{}, snap)
	if err != nil {
		panic(err)
	}
	return *ref
}

// Check ensures that snapshot is valid.
func (o *Operator) Check(ctx context.Context, s cadata.Store, snap Snapshot, checkRoot func(gotfs.Root) error) error {
	logrus.Infof("checking commit #%d", snap.N)
	if err := checkRoot(snap.Root); err != nil {
		return err
	}
	if snap.Parent == nil {
		return nil
	}
	parent, err := o.GetSnapshot(ctx, s, *snap.Parent)
	if err != nil {
		return err
	}
	return o.Check(ctx, s, *parent, checkRoot)
}
