package gotiam

import (
	"regexp"

	"github.com/inet256/inet256/pkg/inet256"
)

type PeerID = inet256.Addr

const (
	OpLook  = "LOOK"
	OpTouch = "TOUCH"
)

type Policy struct {
	Rules []Rule
}

func (p Policy) CanTouchAny(peerID PeerID) (ret bool) {
	// can cas any cell
	for _, r := range p.Rules {
		ret = ret || (r.Subject == peerID && r.Verb == OpTouch)
	}
	return ret
}

func (p Policy) CanLookAny(peerID PeerID) (ret bool) {
	// can get any cell
	for _, r := range p.Rules {
		ret = ret || (r.Subject == peerID && r.Verb == OpLook)
	}
	return ret
}

func (p Policy) CanTouch(peerID PeerID, name string) (ret bool) {
	return p.canDo(peerID, OpTouch, name)
}

func (p Policy) CanLook(peerID PeerID, name string) (ret bool) {
	return p.canDo(peerID, OpLook, name)
}

func (p Policy) canDo(peerID PeerID, method, object string) (ret bool) {
	ret = false
	for _, r := range p.Rules {
		if r.Allows(peerID, method, object) {
			ret = true
		}
		if r.Denies(peerID, method, object) {
			return false
		}
	}
	return ret
}

type Rule struct {
	Allow   bool
	Subject PeerID
	Verb    string
	Object  *regexp.Regexp
}

func (r Rule) Matches(sub PeerID, method, obj string) bool {
	return sub == r.Subject &&
		method == r.Verb &&
		r.Object.MatchString(obj)
}

func (r Rule) Allows(sub PeerID, method, obj string) bool {
	return r.Matches(sub, method, obj) && r.Allow
}

func (r Rule) Denies(sub PeerID, method, obj string) bool {
	return r.Matches(sub, method, obj) && !r.Allow
}
