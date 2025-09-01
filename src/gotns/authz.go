package gotns

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/state/cadata"
)

type Verb string

const (
	Verb_LOOK   Verb = "LOOK"
	Verb_TOUCH  Verb = "TOUCH"
	Verb_CREATE Verb = "CREATE"
	Verb_DROP   Verb = "DROP"
)

type Rule struct {
	// Subject is the name of the group that this rule applies to.
	Subject string
	// Action is the action granted by this rule.
	Verb Verb
	// Object is a regular expression that matches the names of the objects that this rule applies to.
	Object ObjectSet
}

func PostRule(ctx context.Context, s stores.RW, r *Rule) (CID, error) {
	data, err := json.Marshal(r)
	if err != nil {
		return cadata.ID{}, err
	}
	return s.Post(ctx, data)
}

// AddRule adds a rule to the state if it doesn't already exist.
func (m *Machine) AddRule(ctx context.Context, s stores.RW, state State, r *Rule) (State, error) {
	cid, err := PostRule(ctx, s, r)
	if err != nil {
		return State{}, err
	}
	kvr, err := m.gotkv.Put(ctx, s, state.Rules, cid[:], nil)
	if err != nil {
		return State{}, err
	}
	state.Rules = *kvr
	return state, nil
}

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
	if err := json.Unmarshal(data, &rule); err != nil {
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

type CID = cadata.ID

// ObjectSet is a set of objects referred to in a Rule.
// Type is either "group" or "branch"
// Names is a regular expression that defines a set of names.
type ObjectSet struct {
	Type  string
	Names *regexp.Regexp
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
