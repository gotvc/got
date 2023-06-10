package gothost

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/gotvc/got/pkg/gotauthz"
	"github.com/gotvc/got/pkg/gotfs"
)

// GetPolicy reads a Policy from a gotfs filesystem
func GetPolicy(ctx context.Context, op *gotfs.Operator, ms, ds cadata.Store, x gotfs.Root) (*Policy, error) {
	r, err := op.NewReader(ctx, ms, ds, x, PolicyPath)
	if err != nil {
		if posixfs.IsErrNotExist(err) {
			return &Policy{}, nil
		}
		return nil, err
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return ParsePolicy(data)
}

// SetPolicy writes a policy to a gotfs filesystem
func SetPolicy(ctx context.Context, op *gotfs.Operator, ms, ds cadata.Store, x gotfs.Root, pol Policy) (*gotfs.Root, error) {
	data := MarshalPolicy(pol)
	return op.PutFile(ctx, ms, ds, x, PolicyPath, bytes.NewReader(data))
}

type Policy struct {
	Rules []Rule
}

func (p Policy) CanDo(peer PeerID, verb gotauthz.Verb, name string) bool {
	if IsWriteVerb(verb) {
		if name != "" {
			if p.CanTouch(peer, name) {
				return true
			}
		} else {
			if p.CanTouchAny(peer) {
				return true
			}
		}
	} else {
		if name != "" {
			if p.CanLook(peer, name) || p.CanTouch(peer, name) {
				return true
			}
		} else {
			if p.CanLookAny(peer) || p.CanTouchAny(peer) {
				return true
			}
		}
	}
	return false
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

func (p Policy) Clone() Policy {
	return Policy{Rules: append([]Rule{}, p.Rules...)}
}

func IsWriteVerb(v gotauthz.Verb) bool {
	switch v {
	// Branches
	case gotauthz.Verb_Create, gotauthz.Verb_Delete, gotauthz.Verb_Set:
		return true
	case gotauthz.Verb_Get, gotauthz.Verb_List:
		return false

	// Cells
	case gotauthz.Verb_CASCell:
		return true
	case gotauthz.Verb_ReadCell:
		return false

	// Stores
	case gotauthz.Verb_PostBlob, gotauthz.Verb_DeleteBlob:
		return true
	case gotauthz.Verb_GetBlob, gotauthz.Verb_ExistsBlob, gotauthz.Verb_ListBlob:
		return false

	default:
		panic(v)
	}
}

const (
	OpLook  = "LOOK"
	OpTouch = "TOUCH"
)

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

func ParsePolicy(data []byte) (*Policy, error) {
	lines := bytes.Split(data, []byte("\n"))
	var rules []Rule
	for i, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		r, err := ParseRule(line)
		if err != nil {
			return nil, fmt.Errorf("%w while parsing line %d", err, i)
		}
		rules = append(rules, *r)
	}
	return &Policy{Rules: rules}, nil
}

func MarshalPolicy(p Policy) []byte {
	var lines [][]byte
	for _, r := range p.Rules {
		lines = append(lines, MarshalRule(r))
	}
	return bytes.Join(lines, []byte("\n"))
}

func ParseRule(data []byte) (*Rule, error) {
	parts := bytes.Fields(data)
	if len(parts) < 4 {
		return nil, fmt.Errorf("too few fields")
	}
	if len(parts) > 4 {
		return nil, fmt.Errorf("too many fields")
	}
	r := Rule{}
	// Allow/Deny
	switch string(parts[0]) {
	case "ALLOW":
		r.Allow = true
	case "DENY":
		r.Allow = false
	default:
		return nil, fmt.Errorf("rule must start with ALLOW or DENY")
	}
	// Subject
	id := PeerID{}
	if err := id.UnmarshalText(parts[1]); err != nil {
		return nil, err
	}
	r.Subject = id
	// Verb
	switch string(parts[2]) {
	case OpLook, OpTouch:
		r.Verb = string(parts[2])
	default:
		return nil, fmt.Errorf("invalid method %s", string(parts[0]))
	}
	// Object
	re, err := regexp.Compile(string(parts[3]))
	if err != nil {
		return nil, err
	}
	r.Object = re
	return &r, nil
}

func MarshalRule(r Rule) []byte {
	var action string
	if r.Allow {
		action = "ALLOW"
	} else {
		action = "DENY"
	}
	parts := []string{action, r.Subject.String(), r.Verb, r.Object.String()}
	s := strings.Join(parts, "\t")
	return []byte(s)
}
