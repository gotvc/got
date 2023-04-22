package gothost

import (
	"context"
	"sync"

	"github.com/brendoncarroll/go-state/cadata"
	"golang.org/x/exp/slices"

	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/gotauthz"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotvc"
	"github.com/gotvc/got/pkg/stores"
)

type HostEngine struct {
	inner  branches.Space
	owners []PeerID

	vcop *gotvc.Operator
	fsop *gotfs.Operator

	mu           sync.Mutex
	cachedPolicy *Policy
}

func NewHostEngine(inner branches.Space, owners []PeerID) *HostEngine {
	return &HostEngine{
		inner:  inner,
		owners: owners,
	}
}

// Initialize creates the host config branch if it does not exist.
func (e *HostEngine) Initialize(ctx context.Context) error {
	md := branches.NewMetadata(true)
	md.Annotations = append(md.Annotations, branches.Annotation{Key: "protocol", Value: "gothost@v0"})
	b, err := branches.CreateIfNotExists(ctx, e.inner, HostConfigKey, md)
	if err != nil {
		return err
	}
	e.fsop = branches.NewGotFS(b)
	e.vcop = branches.NewGotVC(b)
	return e.reloadPolicy(ctx)
}

func (e *HostEngine) reloadPolicy(ctx context.Context) error {
	pol, err := e.GetPolicy(ctx)
	if err != nil {
		return err
	}
	e.mu.Lock()
	e.cachedPolicy = pol
	e.mu.Unlock()
	return nil
}

func (e *HostEngine) Open(peerID PeerID) branches.Space {
	space := Space{e.inner}
	if slices.Contains(e.owners, peerID) {
		return space
	}
	return gotauthz.NewSpace(space, e.getPolicy(), peerID)
}

func (e *HostEngine) GetPolicy(ctx context.Context) (*Policy, error) {
	ms, ds, root, err := e.readFS(ctx)
	if err != nil {
		return nil, err
	}
	return GetPolicy(ctx, e.fsop, ms, ds, *root)
}

func (e *HostEngine) UpdatePolicy(ctx context.Context, fn func(pol Policy) Policy) error {
	defer e.reloadPolicy(ctx)
	return e.modifyFS(ctx, func(op *gotfs.Operator, ms cadata.Store, ds cadata.Store, x gotfs.Root) (*gotfs.Root, error) {
		xPol, err := GetPolicy(ctx, op, ms, ds, x)
		if err != nil {
			return nil, err
		}
		yPol := fn(*xPol)
		return SetPolicy(ctx, op, ms, ds, x, yPol)
	})
}

func (e *HostEngine) CreateIdentity(ctx context.Context, name string, iden Identity) error {
	return e.modifyFS(ctx, func(op *gotfs.Operator, ms, ds cadata.Store, x gotfs.Root) (*gotfs.Root, error) {
		return CreateIdentity(ctx, op, ms, ds, x, name, iden)
	})
}

func (e *HostEngine) DeleteIdentity(ctx context.Context, name string) error {
	return e.modifyFS(ctx, func(op *gotfs.Operator, ms, ds cadata.Store, x gotfs.Root) (*gotfs.Root, error) {
		return DeleteIdentity(ctx, op, ms, x, name)
	})
}

func (e *HostEngine) GetIdentity(ctx context.Context, name string) (*Identity, error) {
	ms, ds, r, err := e.readFS(ctx)
	if err != nil {
		return nil, err
	}
	return GetIdentity(ctx, e.fsop, ms, ds, *r, name)
}

func (e *HostEngine) ListIdentities(ctx context.Context) ([]string, error) {
	ms, _, r, err := e.readFS(ctx)
	if err != nil {
		return nil, err
	}
	return ListIdentities(ctx, e.fsop, ms, *r)
}

func (e *HostEngine) getPolicy() gotauthz.Policy {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.cachedPolicy
}

func (e *HostEngine) readFS(ctx context.Context) (ms, ds cadata.Store, root *gotfs.Root, _ error) {
	b, err := e.inner.Get(ctx, HostConfigKey)
	if err != nil {
		return nil, nil, nil, err
	}
	snap, err := branches.GetHead(ctx, *b)
	if err != nil {
		return nil, nil, nil, err
	}
	if snap == nil {
		ms := stores.NewMem()
		ds := stores.NewMem()
		root, err := e.fsop.NewEmpty(ctx, ms)
		if err != nil {
			return nil, nil, nil, err
		}
		return ms, ds, root, nil
	}
	return b.Volume.FSStore, b.Volume.RawStore, &snap.Root, nil
}

func (e *HostEngine) modifyFS(ctx context.Context, fn func(op *gotfs.Operator, ms, ds cadata.Store, x gotfs.Root) (*gotfs.Root, error)) error {
	b, err := e.inner.Get(ctx, HostConfigKey)
	if err != nil {
		return err
	}
	scratch := branches.StoreTriple{
		Raw: stores.AddWriteLayer(b.Volume.RawStore, stores.NewMem()),
		FS:  stores.AddWriteLayer(b.Volume.FSStore, stores.NewMem()),
		VC:  stores.AddWriteLayer(b.Volume.VCStore, stores.NewMem()),
	}
	return branches.Apply(ctx, *b, scratch, func(snap *gotvc.Snapshot) (*gotvc.Snapshot, error) {
		var x gotfs.Root
		if snap != nil {
			x = snap.Root
		} else {
			r, err := e.fsop.NewEmpty(ctx, scratch.FS)
			if err != nil {
				return nil, err
			}
			x = *r
		}
		y, err := fn(e.fsop, scratch.FS, scratch.Raw, x)
		if err != nil {
			return nil, err
		}
		var parents []gotvc.Snap
		if snap != nil {
			parents = append(parents, *snap)
		}
		return e.vcop.NewSnapshot(ctx, scratch.VC, parents, *y, gotvc.SnapInfo{})
	})
}
