package gotnsop

import (
	"context"
	"regexp"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/internal/sbe"
	"github.com/gotvc/got/src/internal/stores"
)

type RuleID = blobcache.CID

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

type ObjectType string

const (
	ObjectType_GROUP  ObjectType = "group"
	ObjectType_BRANCH ObjectType = "branch"
	ObjectType_RULE   ObjectType = "rule"
)

type Rule struct {
	// Subject is the name of the group that this rule applies to.
	Subject string
	// Action is the action granted by this rule.
	Verb Verb
	// ObjectType is the type of the object that this rule applies to.
	ObjectType ObjectType
	// Names is a regular expression that matches the names of the objects that this rule applies to.
	Names *regexp.Regexp
}

func (r Rule) ID() RuleID {
	return gdat.Hash(r.Marshal(nil))
}

func (r *Rule) Unmarshal(data []byte) error {
	subject, data, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	verb, data, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	objType, data, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	namesData, _, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	namesRe, err := regexp.Compile(string(namesData))
	if err != nil {
		return err
	}
	r.Subject = string(subject)
	r.Verb = Verb(verb)
	r.ObjectType = ObjectType(objType)
	r.Names = namesRe
	return nil
}

func (r Rule) Marshal(out []byte) []byte {
	out = sbe.AppendLP(out, []byte(r.Subject))
	out = sbe.AppendLP(out, []byte(r.Verb))
	out = sbe.AppendLP(out, []byte(r.ObjectType))
	out = sbe.AppendLP(out, []byte(r.Names.String()))
	return out
}

func (r Rule) Matches(subject string, verb Verb, objType ObjectType, objName string) bool {
	return r.Subject == subject && r.Verb == verb && r.ObjectType == objType && r.Names.MatchString(objName)
}

func PostRule(ctx context.Context, s stores.RW, r *Rule) (blobcache.CID, error) {
	return s.Post(ctx, r.Marshal(nil))
}
