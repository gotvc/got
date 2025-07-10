package gothost

import (
	"fmt"
	"reflect"
	"regexp"

	"github.com/gotvc/got/src/branches/branchintc"
	"github.com/gotvc/got/src/gotauthz"
)

// Role is a set of Actions
type Role struct {
	Single     *Action       `json:"single,omitempty"`
	Regexp     *RegexpRole   `json:"regexp,omitempty"`
	Everything *struct{}     `json:"everything,omitempty"`
	Named      *string       `json:"named,omitempty"`
	Union      []Role        `json:"union,omitempty"`
	Subtract   *SubtractRole `json:"subtract,omitempty"`
}

func (r Role) String() string {
	switch {
	case r.Single != nil:
		return fmt.Sprintf("{single: %v}", *r.Single)
	case r.Regexp != nil:
		return fmt.Sprintf("{regexp: %v}", *r.Regexp)
	case r.Everything != nil:
		return "{everything}"
	case r.Named != nil:
		return fmt.Sprintf("{named: %v}", *r.Named)
	case r.Union != nil:
		return fmt.Sprintf("{union: %v}", r.Union)
	case r.Subtract != nil:
		return fmt.Sprintf("{subtract: %v}", *r.Subtract)
	default:
		return "{empty}"
	}
}

func (r Role) Equals(other Role) bool {
	return reflect.DeepEqual(r, other)
}

func NewRegexpRole(verbs, objs *regexp.Regexp) Role {
	return Role{
		Regexp: &RegexpRole{
			Verbs:   verbs,
			Objects: objs,
		},
	}
}

func NewUnionRole(roles ...Role) Role {
	return Role{Union: roles}
}

func NewNamedRole(name string) Role {
	return Role{Named: &name}
}

func NewSubtractRole(a, b Role) Role {
	return Role{Subtract: &SubtractRole{L: a, R: b}}
}

func Everything() Role {
	return Role{Everything: &struct{}{}}
}

// Subtract is L - R
type SubtractRole struct {
	L Role `json:"l"`
	R Role `json:"r"`
}

// Action is the atomic unit of a Role.
// Roles define sets of Actions.
type Action struct {
	Verb   gotauthz.Verb `json:"verb"`
	Object string        `json:"object"`
}

func (a Action) Contains(x Action) bool {
	return a.Verb == x.Verb && a.Object == x.Object
}

// RegexpRole is a role defined by Regexps on verbs and objects
type RegexpRole struct {
	Verbs   *regexp.Regexp `json:"verbs"`
	Objects *regexp.Regexp `json:"objects"`
}

func (r RegexpRole) Contains(x Action) bool {
	return r.Verbs.MatchString(string(x.Verb)) && r.Objects.MatchString(x.Object)
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
