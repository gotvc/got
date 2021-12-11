package gotrepo

import (
	"bytes"
	"context"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/gotiam"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/pkg/errors"
)

type PeerID = inet256.Addr

func ParsePolicy(data []byte) (*gotiam.Policy, error) {
	lines := bytes.Split(data, []byte("\n"))
	var rules []gotiam.Rule
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
	return &gotiam.Policy{Rules: rules}, nil
}

func MarshalIAMPolicy(p gotiam.Policy) []byte {
	var lines [][]byte
	for _, r := range p.Rules {
		lines = append(lines, MarshalIAMRule(r))
	}
	return bytes.Join(lines, []byte("\n"))
}

func ParseRule(data []byte) (*gotiam.Rule, error) {
	parts := bytes.SplitN(data, []byte(" "), 4)
	if len(parts) < 4 {
		return nil, errors.Errorf("too few fields")
	}
	r := gotiam.Rule{}
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
	id := PeerID{}
	if err := id.UnmarshalText(parts[1]); err != nil {
		return nil, err
	}
	r.Subject = id
	// Verb
	switch string(parts[2]) {
	case gotiam.OpLook, gotiam.OpTouch:
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

func MarshalIAMRule(r gotiam.Rule) []byte {
	var action string
	if r.Allow {
		action = "ALLOW"
	} else {
		action = "DENY"
	}
	parts := []string{action, r.Subject.String(), r.Method, r.Object.String()}
	s := strings.Join(parts, " ")
	return []byte(s)
}

type iamEngine struct {
	repoFS posixfs.FS

	mu     sync.RWMutex
	policy gotiam.Policy
}

func newIAMEngine(repoFS posixfs.FS) (*iamEngine, error) {
	e := &iamEngine{repoFS: repoFS}
	pol, err := e.loadPolicy()
	if err != nil {
		return nil, err
	}
	e.policy = *pol
	return e, nil
}

func (e *iamEngine) loadPolicy() (*gotiam.Policy, error) {
	p := filepath.FromSlash(policyPath)
	data, err := posixfs.ReadFile(context.TODO(), e.repoFS, p)
	if err != nil {
		return nil, err
	}
	return ParsePolicy(data)
}

func (e *iamEngine) savePolicy(x gotiam.Policy) error {
	p := filepath.FromSlash(policyPath)
	data := MarshalIAMPolicy(e.policy)
	return posixfs.PutFile(context.TODO(), e.repoFS, p, 0644, bytes.NewReader(data))
}

func (e *iamEngine) getPolicy() gotiam.Policy {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.policy
}

func (e *iamEngine) Update(fn func(x gotiam.Policy) gotiam.Policy) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	x := gotiam.Policy{Rules: append([]gotiam.Rule{}, e.policy.Rules...)}
	y := fn(x)
	if err := e.savePolicy(y); err != nil {
		return err
	}
	pol, err := e.loadPolicy()
	if err != nil {
		return err
	}
	e.policy = *pol
	return nil
}

func (e *iamEngine) GetSpace(x branches.Space, peerID PeerID) branches.Space {
	return gotiam.NewSpace(x, e.getPolicy(), peerID)
}
