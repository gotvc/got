package got

import (
	"bytes"
	"regexp"
	"strings"

	"github.com/brendoncarroll/go-p2p"
	"github.com/pkg/errors"
)

const (
	methodWrite = "WRITE"
	methodRead  = "READ"
)

type Policy struct {
	rules []Rule
}

func (p Policy) CanWriteAny(peerID p2p.PeerID) (ret bool) {
	// can cas any cell
	for _, r := range p.rules {
		ret = ret || (r.Subject == peerID && r.Method == methodWrite)
	}
	return ret
}

func (p Policy) CanReadAny(peerID p2p.PeerID) (ret bool) {
	// can get any cell
	for _, r := range p.rules {
		ret = ret || (r.Subject == peerID && r.Method == methodRead)
	}
	return ret
}

func (p Policy) CanWrite(peerID p2p.PeerID, name string) (ret bool) {
	return p.canDo(peerID, methodWrite, name)
}

func (p Policy) CanRead(peerID p2p.PeerID, name string) (ret bool) {
	return p.canDo(peerID, methodRead, name)
}

func (p Policy) canDo(peerID p2p.PeerID, method, object string) (ret bool) {
	ret = false
	for _, r := range p.rules {
		if r.Allows(peerID, method, object) {
			ret = true
		}
		if r.Denies(peerID, method, object) {
			return false
		}
	}
	return ret
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
			return nil, errors.Wrapf(err, "error parsing line %d", i)
		}
		rules = append(rules, *r)
	}
	return &Policy{
		rules: rules,
	}, nil
}

func (p Policy) Marshal() []byte {
	var lines [][]byte
	for _, r := range p.rules {
		lines = append(lines, r.Marshal())
	}
	return bytes.Join(lines, []byte("\n"))
}

type Rule struct {
	Allow   bool
	Subject p2p.PeerID
	Method  string
	Object  *regexp.Regexp
}

func ParseRule(data []byte) (*Rule, error) {
	parts := bytes.SplitN(data, []byte(" "), 4)
	if len(parts) < 4 {
		return nil, errors.Errorf("too few fields")
	}
	r := Rule{}
	// Allow/Deny
	switch string(parts[0]) {
	case "ALLOW":
		r.Allow = true
	case "DENY":
		r.Allow = false
	default:
		return nil, errors.Errorf("rule must start with ALLOW or DENY")
	}
	// Subject
	id := p2p.PeerID{}
	if err := id.UnmarshalText(parts[1]); err != nil {
		return nil, err
	}
	r.Subject = id
	// Verb
	switch string(parts[2]) {
	case methodWrite, methodRead:
		r.Method = string(parts[1])
	default:
		return nil, errors.Errorf("invalid method %s", string(parts[0]))
	}
	// Object
	re, err := regexp.Compile(string(parts[3]))
	if err != nil {
		return nil, err
	}
	r.Object = re
	return &r, nil
}

func (r Rule) Matches(sub p2p.PeerID, method, obj string) bool {
	return sub == r.Subject &&
		method == r.Method &&
		r.Object.MatchString(obj)
}

func (r Rule) Allows(sub p2p.PeerID, method, obj string) bool {
	return r.Matches(sub, method, obj) && r.Allow
}

func (r Rule) Denies(sub p2p.PeerID, method, obj string) bool {
	return r.Matches(sub, method, obj) && !r.Allow
}

func (r Rule) MarshalText() ([]byte, error) {
	var action string
	if r.Allow {
		action = "ALLOW"
	} else {
		action = "DENY"
	}
	parts := []string{action, r.Subject.String(), r.Method, r.Object.String()}
	s := strings.Join(parts, " ")
	return []byte(s), nil
}

func (r Rule) Marshal() []byte {
	data, _ := r.MarshalText()
	return data
}
