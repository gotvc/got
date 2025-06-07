package gotauthz

import (
	"context"
	"fmt"

	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/branches/branchintc"
	"go.inet256.org/inet256/pkg/inet256"
)

type (
	PeerID = inet256.Addr
	Verb   = branchintc.Verb
)

// Policy regulates access to a branches.Space
type Policy interface {
	CanDo(sub PeerID, verb Verb, obj string) bool
}

type PolicyFunc func(sub PeerID, verb Verb, obj string) bool

func (pf PolicyFunc) CanDo(sub PeerID, verb Verb, obj string) bool {
	return pf(sub, verb, obj)
}

func NewSpace(x branches.Space, pol Policy, sub PeerID) branches.Space {
	return branchintc.New(x, func(ctx context.Context, verb Verb, obj string, next func(context.Context) error) error {
		if err := Check(pol, sub, verb, obj); err != nil {
			return err
		}
		return next(ctx)
	})
}

func Check(pol Policy, sub PeerID, verb Verb, obj string) error {
	if !pol.CanDo(sub, verb, obj) {
		return ErrNotAllowed{
			Subject: sub,
			Verb:    verb,
			Object:  obj,
		}
	}
	return nil
}

type ErrNotAllowed struct {
	Subject PeerID
	Verb    Verb
	Object  string
}

func (e ErrNotAllowed) Error() string {
	return fmt.Sprintf("%v cannot perform %s on %s", e.Subject, e.Verb, e.Object)
}
