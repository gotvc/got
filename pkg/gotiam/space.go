package gotiam

import (
	"context"
	"fmt"

	"github.com/gotvc/got/pkg/branches"
)

var _ branches.Space = &Space{}

type Space struct {
	inner  branches.Space
	pol    Policy
	peerID PeerID
}

func NewSpace(x branches.Space, pol Policy, peerID PeerID) *Space {
	return &Space{
		inner:  x,
		pol:    pol,
		peerID: peerID,
	}
}

func (s *Space) Create(ctx context.Context, k string, md branches.Metadata) (*branches.Branch, error) {
	if err := s.checkACL("CREATE", true, k); err != nil {
		return nil, err
	}
	return s.inner.Create(ctx, k, md)
}

func (s *Space) Delete(ctx context.Context, k string) error {
	if err := s.checkACL("DELETE", true, k); err != nil {
		return err
	}
	return s.inner.Delete(ctx, k)
}

func (s *Space) Get(ctx context.Context, k string) (*branches.Branch, error) {
	if err := s.checkACL("GET", false, k); err != nil {
		return nil, err
	}
	b, err := s.inner.Get(ctx, k)
	if err != nil {
		return nil, err
	}
	return s.wrapBranch(b, k), nil
}

func (s *Space) Set(ctx context.Context, k string, md branches.Metadata) error {
	if err := s.checkACL("SET", true, k); err != nil {
		return err
	}
	return s.inner.Set(ctx, k, md)
}

func (s *Space) List(ctx context.Context, span branches.Span, limit int) ([]string, error) {
	if err := s.checkACL("LIST", false, ""); err != nil {
		return nil, err
	}
	return s.inner.List(ctx, span, limit)
}

func (s *Space) checkACL(verb string, write bool, name string) error {
	return checkACL(s.pol, s.peerID, name, write, verb)
}

func (s *Space) wrapBranch(x *branches.Branch, name string) *branches.Branch {
	y := *x
	y.Volume = s.wrapVolume(y.Volume, name)
	return &y
}

func (s *Space) wrapVolume(x branches.Volume, name string) branches.Volume {
	check := func(write bool, desc string) error {
		return checkACL(s.pol, s.peerID, name, write, desc)
	}
	return branches.Volume{
		Cell:     newCell(x.Cell, check),
		VCStore:  newStore(x.VCStore, check),
		FSStore:  newStore(x.FSStore, check),
		RawStore: newStore(x.RawStore, check),
	}
}

type checkFn = func(write bool, desc string) error

type ErrNotAllowed struct {
	Subject      PeerID
	Verb, Object string
}

func (e ErrNotAllowed) Error() string {
	return fmt.Sprintf("%v cannot perform %s on %s", e.Subject, e.Verb, e.Object)
}

func checkACL(pol Policy, peer PeerID, name string, write bool, verb string) error {
	if write {
		if name != "" {
			if pol.CanTouch(peer, name) {
				return nil
			}
		} else {
			if pol.CanTouchAny(peer) {
				return nil
			}
		}
	} else {
		if name != "" {
			if pol.CanLook(peer, name) || pol.CanTouch(peer, name) {
				return nil
			}
		} else {
			if pol.CanLookAny(peer) || pol.CanTouchAny(peer) {
				return nil
			}
		}
	}
	return ErrNotAllowed{
		Subject: peer,
		Verb:    verb,
		Object:  name,
	}
}
