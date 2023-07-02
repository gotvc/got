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
	"github.com/gotvc/got/pkg/branches/branchintc"
	"github.com/gotvc/got/pkg/gotfs"
	"golang.org/x/exp/slices"
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

func (p Policy) Equals(q Policy) bool {
	return slices.Equal(p.Rules, q.Rules)
}

func (p Policy) Clone() Policy {
	return Policy{Rules: append([]Rule{}, p.Rules...)}
}

func IsWriteVerb(v branchintc.Verb) bool {
	switch v {
	// Branches
	case branchintc.Verb_Create, branchintc.Verb_Delete, branchintc.Verb_Set:
		return true
	case branchintc.Verb_Get, branchintc.Verb_List:
		return false

	// Cells
	case branchintc.Verb_CASCell:
		return true
	case branchintc.Verb_ReadCell:
		return false

	// Stores
	case branchintc.Verb_PostBlob, branchintc.Verb_DeleteBlob:
		return true
	case branchintc.Verb_GetBlob, branchintc.Verb_ExistsBlob, branchintc.Verb_ListBlob:
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
	Subject IdentityElement
	Verb    string
	Object  *regexp.Regexp
}

func NewRule(allow bool, sub IdentityElement, verb string, obj *regexp.Regexp) Rule {
	return Rule{allow, sub, verb, obj}
}

func (r Rule) String() string {
	return string(MarshalRule(r))
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
	iden, err := ParseIDElement(parts[1])
	if err != nil {
		return nil, err
	}
	r.Subject = iden
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
