package gotns

import (
	"context"
	"fmt"
	"regexp"

	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/state/cadata"
	"go.inet256.org/inet256/src/inet256"
)

type Verb string

const (
	// Verb_LOOK allows a subject to read an object.
	Verb_LOOK Verb = "LOOK"
	// Verb_TOUCH allows a subject to write to an object.
	Verb_TOUCH Verb = "TOUCH"
	// Verb_CREATE allows a subject to create an object with a certain set of names.
	Verb_CREATE Verb = "CREATE"
	// Verb_DROP allows a subject to delete an object with a certain set of names.
	Verb_DROP Verb = "DROP"
	// Verb_ADMIN allows a subject to create rules that reference a set of objects.
	Verb_ADMIN Verb = "ADMIN"
)

type Rule struct {
	// Subject is the name of the group that this rule applies to.
	Subject string
	// Action is the action granted by this rule.
	Verb Verb
	// Object is a regular expression that matches the names of the objects that this rule applies to.
	Object ObjectSet
}

func (r *Rule) Unmarshal(data []byte) error {
	subject, data, err := readLP(data)
	if err != nil {
		return err
	}
	verb, data, err := readLP(data)
	if err != nil {
		return err
	}
	objectData, _, err := readLP(data)
	if err != nil {
		return err
	}
	var objSet ObjectSet
	if err := objSet.Unmarshal(objectData); err != nil {
		return err
	}
	r.Subject = string(subject)
	r.Verb = Verb(verb)
	r.Object = objSet
	return nil
}

func (r Rule) Marshal(out []byte) []byte {
	out = appendLP(out, []byte(r.Subject))
	out = appendLP(out, []byte(r.Verb))
	out = appendLP(out, r.Object.Marshal(nil))
	return out
}

func (r Rule) Matches(subject string, verb Verb, objType, objName string) bool {
	return r.Subject == subject && r.Verb == verb && r.Object.Contains(objType, objName)
}

func PostRule(ctx context.Context, s stores.RW, r *Rule) (CID, error) {
	return s.Post(ctx, r.Marshal(nil))
}

// AddRule adds a rule to the state if it doesn't already exist.
// If it does exist, it does nothing.
func (m *Machine) AddRule(ctx context.Context, s stores.RW, state State, r *Rule) (*State, error) {
	cid, err := PostRule(ctx, s, r)
	if err != nil {
		return nil, err
	}
	kvr, err := m.gotkv.Put(ctx, s, state.Rules, cid[:], nil)
	if err != nil {
		return nil, err
	}
	state.Rules = *kvr
	return &state, nil
}

// DropRule deletes a rule from the state if it exists.
// If it does not exist, it does nothing.
func (m *Machine) DropRule(ctx context.Context, s stores.RW, state State, ruleID CID) (State, error) {
	kvr, err := m.gotkv.Delete(ctx, s, state.Rules, ruleID[:])
	if err != nil {
		return State{}, err
	}
	state.Rules = *kvr
	return state, nil
}

func (m *Machine) GetRule(ctx context.Context, s stores.Reading, state State, cid CID) (Rule, error) {
	const MaxRuleSize = 1024
	buf := make([]byte, MaxRuleSize)
	n, err := s.Get(ctx, cid, buf)
	if err != nil {
		return Rule{}, err
	}
	data := buf[:n]

	var rule Rule
	if err := rule.Unmarshal(data); err != nil {
		return Rule{}, err
	}
	return rule, nil
}

// ForEachRule calls fn for each rule.
func (m *Machine) ForEachRule(ctx context.Context, s stores.Reading, state State, fn func(rule Rule) error) (State, error) {
	if err := m.gotkv.ForEach(ctx, s, state.Rules, gotkv.TotalSpan(), func(ent gotkv.Entry) error {
		k := ent.Key
		if len(k) != 32 {
			return fmt.Errorf("rules table: parsing CID, wrong length: %d", len(k))
		}
		cid := CID(k)
		rule, err := m.GetRule(ctx, s, state, cid)
		if err != nil {
			return err
		}
		return fn(rule)
	}); err != nil {
		return State{}, err
	}
	return state, nil
}

// CanDo returns true if the subject can perform the action on the object.
func (m *Machine) CanDo(ctx context.Context, s stores.Reading, state State, actor inet256.ID, verb Verb, objType, objName string) (bool, error) {
	var allowed bool
	if err := m.gotkv.ForEach(ctx, s, state.Rules, gotkv.TotalSpan(), func(ent gotkv.Entry) error {
		var rule Rule
		if err := rule.Unmarshal(ent.Value); err != nil {
			return err
		}
		if rule.Verb != verb {
			// rule does not apply to this verb
			return nil
		}
		if !rule.Object.Contains(objType, objName) {
			// rule does not apply to this object
			return nil
		}
		if yes, err := m.GroupContains(ctx, s, state, rule.Subject, actor); err != nil {
			return err
		} else if !yes {
			// actor is not a member of the subject group, so even if the rule applies, it doesn't help this actor.
			return nil
		}
		// This may be somewhat redundant, but it's the source of truth.
		// Never set allowed=true unless this function returns true.
		if rule.Matches(rule.Subject, verb, objType, objName) {
			allowed = true
		}
		return nil
	}); err != nil {
		return false, err
	}
	return allowed, nil
}

type CID = cadata.ID

// ObjectSet is a set of objects referred to in a Rule.
// Type is either "group" or "branch"
// Names is a regular expression that defines a set of names.
type ObjectSet struct {
	Type  string
	Names *regexp.Regexp
}

func (o ObjectSet) Contains(objType, objName string) bool {
	return o.Type == objType && o.Names.MatchString(objName)
}

// NewGroupSet refers to a set of groups.
func NewGroupSet(names *regexp.Regexp) ObjectSet {
	return ObjectSet{Type: "group", Names: names}
}

// NewBranchSet refers to a set of branches.
func NewBranchSet(names *regexp.Regexp) ObjectSet {
	return ObjectSet{Type: "branch", Names: names}
}

func AllGroups() ObjectSet {
	return NewGroupSet(regexp.MustCompile(".*"))
}

func (o ObjectSet) Marshal(out []byte) []byte {
	out = appendLP(out, []byte(o.Type))
	out = appendLP(out, []byte(o.Names.String()))
	return out
}

func (o *ObjectSet) Unmarshal(data []byte) error {
	typeData, data, err := readLP(data)
	if err != nil {
		return err
	}
	o.Type = string(typeData)
	namesData, _, err := readLP(data)
	if err != nil {
		return err
	}
	o.Names = regexp.MustCompile(string(namesData))
	return nil
}

func (o ObjectSet) ContainsGroup(group string) bool {
	return o.Type == "group" && o.Names.MatchString(group)
}

func (o ObjectSet) ContainsBranch(branch string) bool {
	return o.Type == "branch" && o.Names.MatchString(branch)
}

func (m *Machine) addInitialRules(ctx context.Context, s stores.RW, state State, adminGroupName string) (*State, error) {
	for _, rule := range []Rule{
		{
			Subject: adminGroupName,
			Verb:    Verb_ADMIN,
			Object:  NewGroupSet(regexp.MustCompile(".*")),
		},
		{
			Subject: adminGroupName,
			Verb:    Verb_ADMIN,
			Object:  NewBranchSet(regexp.MustCompile(".*")),
		},
	} {
		next, err := m.AddRule(ctx, s, state, &rule)
		if err != nil {
			return nil, err
		}
		state = *next
	}
	return &state, nil
}
