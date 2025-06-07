package gothost

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"

	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/state/cells"
	"golang.org/x/sync/semaphore"

	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/branches/branchintc"
	"github.com/gotvc/got/pkg/gotauthz"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotvc"
	"github.com/gotvc/got/pkg/stores"
)

type HostEngine struct {
	inner branches.Space

	vcag *gotvc.Agent
	fsag *gotfs.Agent

	initDone   atomic.Bool
	initSem    *semaphore.Weighted
	cachedCell *cells.Cached[[]byte]
	stateCell  *cells.Derived[[]byte, State]
	policyCell *cells.Derived[State, *authzPolicy]
}

func NewHostEngine(inner branches.Space) *HostEngine {
	info := branches.NewConfig(true).AsInfo()
	return &HostEngine{
		inner:   inner,
		fsag:    branches.NewGotFS(&info),
		vcag:    branches.NewGotVC(&info),
		initSem: semaphore.NewWeighted(1),
	}
}

// Initialize creates the host config branch if it does not exist.
// Initialize only needs to be called on the host.
func (e *HostEngine) Initialize(ctx context.Context) error {
	md := branches.NewConfig(true)
	md.Annotations = append(md.Annotations, branches.Annotation{Key: "protocol", Value: "gothost@v0"})
	_, err := branches.CreateIfNotExists(ctx, e.inner, HostConfigKey, md)
	if err != nil {
		return err
	}
	return e.ensureInit(ctx)
}

// ensureInit should be called before reading from any internal cells.
func (e *HostEngine) ensureInit(ctx context.Context) error {
	if e.initDone.Load() {
		return nil
	}
	if err := e.initSem.Acquire(ctx, 1); err != nil {
		return err
	}
	defer e.initSem.Release(1)
	if e.initDone.Load() {
		return nil
	}
	v, err := e.inner.Open(ctx, HostConfigKey)
	if err != nil {
		return err
	}
	e.cachedCell = cells.NewCached[[]byte](v.Cell)
	e.stateCell = cells.NewDerived[[]byte, State](cells.DerivedParams[[]byte, State]{
		Inner: e.cachedCell,
		Forward: func(ctx context.Context, dst *State, src []byte) error {
			if len(src) == 0 {
				*dst = State{}
				return nil
			}
			var snap gotvc.Snapshot
			if err := json.Unmarshal(src, &snap); err != nil {
				return err
			}
			return dst.Load(ctx, e.fsag, v.FSStore, v.RawStore, snap.Root)
		},
		Inverse: nil,
		Copy:    cells.DefaultCopy[State],
		Equals: func(a, b State) bool {
			return a.Equals(b)
		},
	})
	e.policyCell = cells.NewDerived[State, *authzPolicy](cells.DerivedParams[State, *authzPolicy]{
		Inner: e.stateCell,
		Forward: func(ctx context.Context, dst **authzPolicy, src State) error {
			ap, err := newAuthzPolicy(src)
			*dst = ap
			return err
		},
		Inverse: nil,
		Copy:    cells.DefaultCopy[*authzPolicy],
		Equals:  cells.DefaultEquals[*authzPolicy],
	})
	e.initDone.Store(true)
	return nil
}

func (e *HostEngine) Open(peerID PeerID) branches.Space {
	return branchintc.New(e.inner, func(ctx context.Context, verb branchintc.Verb, obj string, next func(context.Context) error) error {
		if err := e.ensureInit(ctx); err != nil {
			return err
		}
		pol, err := cells.Load[*authzPolicy](ctx, e.policyCell)
		if err != nil {
			return err
		}
		if err := gotauthz.Check(pol, peerID, verb, obj); err != nil {
			return err
		}
		if obj == HostConfigKey {
			switch verb {
			case branchintc.Verb_Create, branchintc.Verb_Delete, branchintc.Verb_Set:
				return newConfigBranchErr()
			case branchintc.Verb_CASCell:
				defer e.cachedCell.Invalidate()
			}
		}
		return next(ctx)
	})
}

func (e *HostEngine) Modify(ctx context.Context, fn func(State) (*State, error)) error {
	return e.modifyFS(ctx, func(op *gotfs.Agent, ms cadata.Store, ds cadata.Store, x gotfs.Root) (*gotfs.Root, error) {
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
	if err := s.Load(ctx, e.fsag, ms, ds, *root); err != nil {
		return nil, err
	}
	return &s, nil
}

func (e *HostEngine) GetPolicy(ctx context.Context) (*Policy, error) {
	ms, ds, root, err := e.readFS(ctx)
	if err != nil {
		return nil, err
	}
	return GetPolicy(ctx, e.fsag, ms, ds, *root)
}

func (e *HostEngine) ModifyPolicy(ctx context.Context, fn func(pol Policy) Policy) error {
	return e.modifyFS(ctx, func(op *gotfs.Agent, ms cadata.Store, ds cadata.Store, x gotfs.Root) (*gotfs.Root, error) {
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
	return e.modifyFS(ctx, func(op *gotfs.Agent, ms, ds cadata.Store, x gotfs.Root) (*gotfs.Root, error) {
		return CreateIdentity(ctx, op, ms, ds, x, name, iden)
	})
}

func (e *HostEngine) DeleteIdentity(ctx context.Context, name string) error {
	return e.modifyFS(ctx, func(op *gotfs.Agent, ms, ds cadata.Store, x gotfs.Root) (*gotfs.Root, error) {
		return DeleteIdentity(ctx, op, ms, x, name)
	})
}

func (e *HostEngine) GetIdentity(ctx context.Context, name string) (*Identity, error) {
	ms, ds, r, err := e.readFS(ctx)
	if err != nil {
		return nil, err
	}
	return GetIdentity(ctx, e.fsag, ms, ds, *r, name)
}

func (e *HostEngine) ListIdentities(ctx context.Context) ([]string, error) {
	ms, _, r, err := e.readFS(ctx)
	if err != nil {
		return nil, err
	}
	return ListIdentities(ctx, e.fsag, ms, *r)
}

func (e *HostEngine) ListIdentitiesFull(ctx context.Context) ([]IDEntry, error) {
	ms, ds, r, err := e.readFS(ctx)
	if err != nil {
		return nil, err
	}
	return ListIdentitiesFull(ctx, e.fsag, ms, ds, *r)
}

func (e *HostEngine) CanDo(ctx context.Context, sub PeerID, verb branchintc.Verb, obj string) (bool, error) {
	if err := e.ensureInit(ctx); err != nil {
		return false, err
	}
	ap, err := cells.Load[*authzPolicy](ctx, e.policyCell)
	if err != nil {
		return false, err
	}
	return ap.CanDo(sub, verb, obj), nil
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
		root, err := e.fsag.NewEmpty(ctx, ms)
		if err != nil {
			return nil, nil, nil, err
		}
		return ms, ds, root, nil
	}
	return v.FSStore, v.RawStore, &snap.Root, nil
}

func (e *HostEngine) modifyFS(ctx context.Context, fn func(op *gotfs.Agent, ms, ds cadata.Store, x gotfs.Root) (*gotfs.Root, error)) error {
	defer func() {
		if e.cachedCell != nil {
			e.cachedCell.Invalidate()
		}
	}()
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
			r, err := e.fsag.NewEmpty(ctx, scratch.FS)
			if err != nil {
				return nil, err
			}
			x = *r
		}
		y, err := fn(e.fsag, scratch.FS, scratch.Raw, x)
		if err != nil {
			return nil, err
		}
		var parents []gotvc.Snap
		if snap != nil {
			parents = append(parents, *snap)
		}
		return e.vcag.NewSnapshot(ctx, scratch.VC, parents, *y, gotvc.SnapInfo{})
	})
}

type authzPolicy struct {
	pol        Policy
	identities map[string]Identity
	roles      map[string]Role
}

func newAuthzPolicy(x State) (*authzPolicy, error) {
	if err := x.Validate(); err != nil {
		return nil, err
	}
	return &authzPolicy{
		pol:        x.Policy,
		roles:      x.Roles,
		identities: x.Identities,
	}, nil
}

func (p *authzPolicy) CanDo(sub PeerID, verb gotauthz.Verb, object string) bool {
	a := Action{Verb: verb, Object: object}
	for _, r := range p.pol.Rules {
		if p.roleContains(r.Role, a) && p.idenContains(r.Identity, sub) {
			return true
		}
	}
	return false
}

// isReachableFrom returns true if dst is reachable from start
// TODO: use graphs package, this is O(n^2)
func (p *authzPolicy) idenContains(iden Identity, x PeerID) bool {
	switch {
	case iden.Peer != nil:
		if *iden.Peer == x {
			return true
		}
	case iden.Name != nil:
		if iden2, exists := p.identities[*iden.Name]; exists {
			return p.idenContains(iden2, x)
		}
	case iden.Anyone != nil:
		return true
	case iden.Union != nil:
		for _, iden2 := range iden.Union {
			if p.idenContains(iden2, x) {
				return true
			}
		}
	}
	return false
}

func (p *authzPolicy) roleContains(role Role, a Action) (ret bool) {
	switch {
	case role.Single != nil:
		return role.Single.Contains(a)
	case role.Regexp != nil:
		return role.Regexp.Contains(a)
	case role.Union != nil:
		for _, role2 := range role.Union {
			if p.roleContains(role2, a) {
				return true
			}
		}
		return false
	case role.Subtract != nil:
		return p.roleContains(role.Subtract.L, a) && !p.roleContains(role.Subtract.R, a)
	case role.Named != nil:
		role2, exists := p.roles[*role.Named]
		if !exists {
			return false
		}
		return p.roleContains(role2, a)
	case role.Everything != nil:
		return true
	default:
		return false
	}
}

func newConfigBranchErr() error {
	return fmt.Errorf("cannot delete %s branch", HostConfigKey)
}
