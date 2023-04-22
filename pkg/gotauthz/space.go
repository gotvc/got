package gotauthz

import (
	"context"
	"fmt"

	"github.com/gotvc/got/pkg/branches"
	"github.com/inet256/inet256/pkg/inet256"
)

var _ branches.Space = &Space{}

type PeerID = inet256.Addr

type Verb string

const (
	Verb_Create = "CREATE"
	Verb_Delete = "DELETE"
	Verb_Get    = "GET"
	Verb_Set    = "SET"
	Verb_List   = "LIST"

	Verb_CASCell  = "CAS_CELL"
	Verb_ReadCell = "READ_CELL"

	Verb_GetBlob    = "GET_BLOB"
	Verb_ListBlob   = "LIST_BLOB"
	Verb_ExistsBlob = "EXISTS_BLOB"
	Verb_PostBlob   = "POST_BLOB"
	Verb_DeleteBlob = "DELETE_BLOB"
)

// Policy regulates access to a branches.Space
type Policy interface {
	CanDo(sub PeerID, verb Verb, obj string) bool
}

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
	if err := s.checkACL("CREATE", k); err != nil {
		return nil, err
	}
	return s.inner.Create(ctx, k, md)
}

func (s *Space) Delete(ctx context.Context, k string) error {
	if err := s.checkACL("DELETE", k); err != nil {
		return err
	}
	return s.inner.Delete(ctx, k)
}

func (s *Space) Get(ctx context.Context, k string) (*branches.Branch, error) {
	if err := s.checkACL("GET", k); err != nil {
		return nil, err
	}
	b, err := s.inner.Get(ctx, k)
	if err != nil {
		return nil, err
	}
	return s.wrapBranch(b, k), nil
}

func (s *Space) Set(ctx context.Context, k string, md branches.Metadata) error {
	if err := s.checkACL("SET", k); err != nil {
		return err
	}
	return s.inner.Set(ctx, k, md)
}

func (s *Space) List(ctx context.Context, span branches.Span, limit int) ([]string, error) {
	if err := s.checkACL("LIST", ""); err != nil {
		return nil, err
	}
	return s.inner.List(ctx, span, limit)
}

func (s *Space) checkACL(verb Verb, obj string) error {
	if s.pol.CanDo(s.peerID, verb, obj) {
		return nil
	}
	return ErrNotAllowed{
		Subject: s.peerID,
		Verb:    verb,
		Object:  obj,
	}
}

func (s *Space) wrapBranch(x *branches.Branch, name string) *branches.Branch {
	y := *x
	y.Volume = s.wrapVolume(y.Volume, name)
	return &y
}

func (s *Space) wrapVolume(x branches.Volume, name string) branches.Volume {
	check := func(v Verb) error {
		return s.checkACL(v, name)
	}
	return branches.Volume{
		Cell:     newCell(x.Cell, check),
		VCStore:  newStore(x.VCStore, check),
		FSStore:  newStore(x.FSStore, check),
		RawStore: newStore(x.RawStore, check),
	}
}

type checkFn = func(verb Verb) error

type ErrNotAllowed struct {
	Subject PeerID
	Verb    Verb
	Object  string
}

func (e ErrNotAllowed) Error() string {
	return fmt.Sprintf("%v cannot perform %s on %s", e.Subject, e.Verb, e.Object)
}
