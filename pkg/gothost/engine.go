package gothost

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/brendoncarroll/go-state/cadata"

	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/branches/branchintc"
	"github.com/gotvc/got/pkg/gotauthz"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotvc"
	"github.com/gotvc/got/pkg/stores"
)

type HostEngine struct {
	inner branches.Space

	vcop *gotvc.Operator
	fsop *gotfs.Operator

	cachedPolicy atomic.Pointer[authzPolicy]
}

func NewHostEngine(inner branches.Space) *HostEngine {
	info := branches.NewConfig(true).AsInfo()
	return &HostEngine{
		inner: inner,
		fsop:  branches.NewGotFS(&info),
		vcop:  branches.NewGotVC(&info),
	}
}

// Initialize creates the host config branch if it does not exist.
func (e *HostEngine) Initialize(ctx context.Context) error {
	md := branches.NewConfig(true)
	md.Annotations = append(md.Annotations, branches.Annotation{Key: "protocol", Value: "gothost@v0"})
	_, err := branches.CreateIfNotExists(ctx, e.inner, HostConfigKey, md)
	if err != nil {
		return err
	}
	return e.reload(ctx)
}

func (e *HostEngine) reload(ctx context.Context) error {
	s, err := e.View(ctx)
	if err != nil {
		return err
	}
	ap, err := newAuthzPolicy(*s)
	if err != nil {
		return err
	}
	e.cachedPolicy.Store(ap)
	return nil
}

func (e *HostEngine) Open(peerID PeerID) branches.Space {
	return branchintc.New(e.inner, func(verb branchintc.Verb, obj string, next func() error) error {
		pol := e.cachedPolicy.Load()
		if err := gotauthz.Check(pol, peerID, verb, obj); err != nil {
			return err
		}
		if obj == HostConfigKey {
			switch verb {
			case branchintc.Verb_Create, branchintc.Verb_Delete, branchintc.Verb_Set:
				return newConfigBranchErr()
			case branchintc.Verb_CASCell:
				// TODO: need to invalidate policy
				defer e.reload(context.TODO())
			}
		}
		return next()
	})
}

func (e *HostEngine) Modify(ctx context.Context, fn func(State) (*State, error)) error {
	return e.modifyFS(ctx, func(op *gotfs.Operator, ms cadata.Store, ds cadata.Store, x gotfs.Root) (*gotfs.Root, error) {
		var xState State
		if err := xState.Load(ctx, op, ms, ds, x); err != nil {
			return nil, err
		}
		yState, err := fn(xState)
		if err != nil {
			return nil, err
		}
		if err := yState.Validate(); err != nil {
			return nil, err
		}
		return yState.Save(ctx, op, ms, ds)
	})
}

func (e *HostEngine) View(ctx context.Context) (*State, error) {
	ms, ds, root, err := e.readFS(ctx)
	if err != nil {
		return nil, err
	}
	var s State
	if err := s.Load(ctx, e.fsop, ms, ds, *root); err != nil {
		return nil, err
	}
	return &s, nil
}

func (e *HostEngine) GetPolicy(ctx context.Context) (*Policy, error) {
	ms, ds, root, err := e.readFS(ctx)
	if err != nil {
		return nil, err
	}
	return GetPolicy(ctx, e.fsop, ms, ds, *root)
}

func (e *HostEngine) ModifyPolicy(ctx context.Context, fn func(pol Policy) Policy) error {
	return e.modifyFS(ctx, func(op *gotfs.Operator, ms cadata.Store, ds cadata.Store, x gotfs.Root) (*gotfs.Root, error) {
		xPol, err := GetPolicy(ctx, op, ms, ds, x)
		if err != nil {
			return nil, err
		}
		yPol := fn(*xPol)
		return SetPolicy(ctx, op, ms, ds, x, yPol)
	})
}

func (e *HostEngine) SetPolicy(ctx context.Context, pol Policy) error {
	return e.ModifyPolicy(ctx, func(Policy) Policy {
		return pol
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

func (e *HostEngine) ListIdentitiesFull(ctx context.Context) ([]IDEntry, error) {
	ms, ds, r, err := e.readFS(ctx)
	if err != nil {
		return nil, err
	}
	return ListIdentitiesFull(ctx, e.fsop, ms, ds, *r)
}

func (e *HostEngine) readFS(ctx context.Context) (ms, ds cadata.Store, root *gotfs.Root, _ error) {
	v, err := e.inner.Open(ctx, HostConfigKey)
	if err != nil {
		return nil, nil, nil, err
	}
	snap, err := branches.GetHead(ctx, *v)
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
	return v.FSStore, v.RawStore, &snap.Root, nil
}

func (e *HostEngine) modifyFS(ctx context.Context, fn func(op *gotfs.Operator, ms, ds cadata.Store, x gotfs.Root) (*gotfs.Root, error)) error {
	defer e.reload(ctx)
	v, err := e.inner.Open(ctx, HostConfigKey)
	if err != nil {
		return err
	}
	scratch := branches.StoreTriple{
		Raw: stores.AddWriteLayer(v.RawStore, stores.NewMem()),
		FS:  stores.AddWriteLayer(v.FSStore, stores.NewMem()),
		VC:  stores.AddWriteLayer(v.VCStore, stores.NewMem()),
	}
	return branches.Apply(ctx, *v, scratch, func(snap *gotvc.Snapshot) (*gotvc.Snapshot, error) {
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

type authzPolicy struct {
	pol        Policy
	identities map[string]Identity
}

func newAuthzPolicy(x State) (*authzPolicy, error) {
	if err := x.Validate(); err != nil {
		return nil, err
	}
	return &authzPolicy{
		pol:        x.Policy,
		identities: x.Identities,
	}, nil
}

func (p *authzPolicy) CanDo(sub PeerID, verb gotauthz.Verb, object string) bool {
	var allow bool
	for _, r := range p.pol.Rules {
		if p.matchesVerb(&r, verb) && p.isReachableFrom(sub, r.Subject) && r.Object.MatchString(object) {
			allow = r.Allow
		}
	}
	return allow
}

// isReachableFrom returns true if dst is reachable from start
// TODO: use graphs package, this is O(n^2)
func (p *authzPolicy) isReachableFrom(dst PeerID, start IdentityElement) bool {
	switch {
	case start.Peer != nil:
		if *start.Peer == dst {
			return true
		}
	case start.Name != nil:
		if iden, exists := p.identities[*start.Name]; exists {
			for _, target2 := range iden.Members {
				if p.isReachableFrom(dst, target2) {
					return true
				}
			}
		}
	case start.Anyone != nil:
		return true
	}
	return false
}

func (p *authzPolicy) matchesVerb(r *Rule, verb gotauthz.Verb) bool {
	switch r.Verb {
	case OpLook:
		return !IsWriteVerb(verb)
	case OpTouch:
		return true
	}
	return false
}

func newConfigBranchErr() error {
	return fmt.Errorf("cannot delete %s branch", HostConfigKey)
}
