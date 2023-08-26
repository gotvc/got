package gothost

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/posixfs"
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
	Rules []Rule `json:"rules"`
}

func (p Policy) Equals(other Policy) bool {
	return slices.EqualFunc(p.Rules, other.Rules, func(a, b Rule) bool {
		return a.Identity.Equals(b.Identity) && a.Role.Equals(b.Role)
	})
}

func (p Policy) Clone() Policy {
	return Policy{Rules: append([]Rule{}, p.Rules...)}
}

func ParsePolicy(data []byte) (*Policy, error) {
	var ret Policy
	if err := json.Unmarshal(data, &ret); err != nil {
		return nil, err
	}
	return &ret, nil
}

func MarshalPolicy(p Policy) []byte {
	data, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}
	return data
}

// Rules joins an Identity to a Role
// It means the peers described by Identity can do the things described by Role.
type Rule struct {
	Identity Identity `json:"identity"`
	Role     Role     `json:"role"`
}

func NewRule(sub Identity, role Role) Rule {
	return Rule{sub, role}
}

func (r Rule) String() string {
	return fmt.Sprintf("(identity=%v, role=%v)", r.Identity, r.Role)
}
