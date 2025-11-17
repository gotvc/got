package gotns

import (
	"context"
	"fmt"
	"regexp"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/gotns/internal/gotnsop"
	"github.com/gotvc/got/src/internal/sbe"
	"github.com/gotvc/got/src/internal/stores"
	"go.inet256.org/inet256/src/inet256"
)

type (
	RuleID     = gotnsop.RuleID
	Rule       = gotnsop.Rule
	Verb       = gotnsop.Verb
	ObjectType = gotnsop.ObjectType
)

// AddRule adds a rule to the state if it doesn't already exist.
// If it does exist, it does nothing.
func (m *Machine) AddRule(ctx context.Context, s stores.RW, state State, r *gotnsop.Rule) (*State, error) {
	cid, err := gotnsop.PostRule(ctx, s, r)
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
		return gotnsop.Rule{}, err
	}
	data := buf[:n]

	var rule gotnsop.Rule
	if err := rule.Unmarshal(data); err != nil {
		return gotnsop.Rule{}, err
	}
	return rule, nil
}

// ForEachRule calls fn for each rule.
func (m *Machine) ForEachRule(ctx context.Context, s stores.Reading, state State, fn func(rule Rule) error) error {
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
		return err
	}
	return nil
}

// CanDo returns true if the subject can perform the action on the object.
func (m *Machine) CanDo(ctx context.Context, s stores.Reading, state State, actor inet256.ID, verb Verb, objType ObjectType, objName string) (bool, error) {
	var allowed bool
	if err := m.gotkv.ForEach(ctx, s, state.Rules, gotkv.TotalSpan(), func(ent gotkv.Entry) error {
		var rule gotnsop.Rule
		if err := rule.Unmarshal(ent.Value); err != nil {
			return err
		}
		if rule.Verb != verb {
			// rule does not apply to this verb
			return nil
		}
		if rule.ObjectType != objType {
			// rule does not apply to this object type
			return nil
		}
		if !rule.Names.MatchString(objName) {
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

func (m *Machine) CanAnyDo(ctx context.Context, s stores.Reading, state State, actors IDSet, verb Verb, objType ObjectType, objName string) (bool, error) {
	for actor := range actors {
		yes, err := m.CanDo(ctx, s, state, actor, verb, objType, objName)
		if err != nil {
			return false, err
		}
		if yes {
			return true, nil
		}
	}
	return false, nil
}

// Obligation associates encrypted secret keys with Volumes, according to rules.
type Obligation struct {
	// HashOfSecret is the hash of the secret that the obligation is for.
	HashOfSecret [32]byte
	// Ratchet is the number of times that the secret is hashed to produce HashOfSecret
	// This value must always be >= 0, usually 1 for readers and 2 for writers.
	Ratchet uint8
	// GroupID is the id of the Group that is entitled to the secret.
	GroupID GroupID

	// The seed can be decrypted with the corresponding private key for the KEMHash.
	// The seed will decrypt to a 64 byte seed, which is used for the volume's Signing Key.
	// The hash of the seed will be used for the symmetric cipher.
	EncryptedSecret []byte
}

func (o *Obligation) Key(out []byte) []byte {
	out = append(out, o.HashOfSecret[:]...)
	out = append(out, o.Ratchet)
	out = append(out, o.GroupID[:]...)
	return out
}

func (o *Obligation) Value(out []byte) []byte {
	out = sbe.AppendLP(out, o.EncryptedSecret)
	return out
}

func ParseObligation(key []byte, value []byte) (*Obligation, error) {
	// key
	if len(key) < 32+32+1 {
		return nil, fmt.Errorf("key too short")
	}
	hos := [32]byte(key[:32])
	ratchet := key[32]
	kemHash := [32]byte(key[33 : 33+32])
	if ratchet == 0 {
		return nil, fmt.Errorf("parsing Obligation: ratchet must be >= 0")
	}

	// value
	encryptedSeed, value, err := sbe.ReadLP(value)
	if err != nil {
		return nil, err
	}

	return &Obligation{
		HashOfSecret: hos,
		Ratchet:      ratchet,
		GroupID:      kemHash,

		EncryptedSecret: encryptedSeed,
	}, nil
}

func (m *Machine) PutObligation(ctx context.Context, s stores.RW, state State, o *Obligation) (*State, error) {
	kvr, err := m.gotkv.Put(ctx, s, state.Obligations, o.Key(nil), o.Value(nil))
	if err != nil {
		return nil, err
	}
	state.Obligations = *kvr
	return &state, nil
}

// ForEachObligation iterates over all Obligations
// If hos is non-nil, then only obligations for the corresponding secret will be emmitted.
func (m *Machine) ForEachObligation(ctx context.Context, s stores.Reading, x State, hos *[32]byte, fn func(ob Obligation) error) error {
	span := gotkv.TotalSpan()
	if hos != nil {
		span = gotkv.PrefixSpan(hos[:])
	}
	return m.gotkv.ForEach(ctx, s, x.Obligations, span, func(ent gotkv.Entry) error {
		oblig, err := ParseObligation(ent.Key, ent.Value)
		if err != nil {
			return err
		}
		return fn(*oblig)
	})
}

// fulfill handles obligations for a single rule.
func (m *Machine) fulfill(ctx context.Context, s stores.RW, x State, rule Rule, secret *gotnsop.Secret) (*State, error) {
	g, err := m.GetGroup(ctx, s, x, rule.Subject)
	if err != nil {
		return nil, err
	}
	kemPub := g.KEM
	hos := secret.Ratchet(2)
	switch rule.Verb {
	case gotnsop.Verb_LOOK:
		// Need to encrypt the secret for this rule.
		lookSecret := secret.Ratchet(1)
		nextState, err := m.PutObligation(ctx, s, x, &Obligation{
			HashOfSecret:    hos,
			GroupID:         g.ID,
			Ratchet:         1,
			EncryptedSecret: encryptSeed(nil, kemPub, &lookSecret),
		})
		if err != nil {
			return nil, err
		}
		x = *nextState
	case gotnsop.Verb_TOUCH, gotnsop.Verb_ADMIN:
		// Need to encrypt the secret for this rule.
		nextState, err := m.PutObligation(ctx, s, x, &Obligation{
			HashOfSecret:    hos,
			GroupID:         g.ID,
			Ratchet:         2,
			EncryptedSecret: encryptSeed(nil, kemPub, secret),
		})
		if err != nil {
			return nil, err
		}
		x = *nextState
	}
	return &x, nil
}

// FulfillObligations ensures that obligations for the entry are satisfied.
func (m *Machine) FulfillObligations(ctx context.Context, s stores.RW, x State, name string, secret *gotnsop.Secret) (*State, error) {
	entry, err := m.GetAlias(ctx, s, x, name)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, fmt.Errorf("alias %s not found", name)
	}
	if err := m.ForEachRule(ctx, s, x, func(rule gotnsop.Rule) error {
		if rule.ObjectType != gotnsop.ObjectType_BRANCH || !rule.Names.MatchString(name) {
			return nil
		}
		y, err := m.fulfill(ctx, s, x, rule, secret)
		if err != nil {
			return err
		}
		x = *y
		return err
	}); err != nil {
		return nil, err
	}
	return &x, nil
}

type CID = blobcache.CID

func (m *Machine) addInitialRules(ctx context.Context, s stores.RW, state State, adminGID GroupID) (*State, error) {
	for _, rule := range []gotnsop.Rule{
		{
			Subject:    adminGID,
			Verb:       gotnsop.Verb_ADMIN,
			ObjectType: gotnsop.ObjectType_GROUP,
			Names:      regexp.MustCompile(".*"),
		},
		{
			Subject:    adminGID,
			Verb:       gotnsop.Verb_ADMIN,
			ObjectType: gotnsop.ObjectType_BRANCH,
			Names:      regexp.MustCompile(".*"),
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

// FindSecret reveres the provied hash.
// The goal motivating all the complexity in this package is just to implement this function.
//  1. Fnd all the obligations that hold the secret we are interested in.
//  2. Find all the groups which can decrypt those secrets.
//  3. Find a path from the the actor's ID to one of those groups.
//  4. Use that path to perform a chain of KEM decryptions eventually resulting
//     a secret value which reverses hash of secret.
func (m *Machine) FindSecret(ctx context.Context, s stores.Reading, x State, actAs IdenPrivate, hos *[32]byte, minRatchet uint8) (*gotnsop.Secret, uint8, error) {
	gids := map[GroupID]uint8{}
	if err := m.ForEachObligation(ctx, s, x, hos, func(oblig Obligation) error {
		if oblig.HashOfSecret == *hos && oblig.Ratchet >= minRatchet {
			gids[oblig.GroupID] = oblig.Ratchet
		}
		return nil
	}); err != nil {
		return nil, 0, err
	}

	// Find the groups with access
	for gid, ratchet := range gids {
		p, err := m.FindGroupPath(ctx, s, x, actAs.GetID(), gid)
		if err != nil {
			return nil, 0, err
		}
		if len(p) == 0 {
			continue
		}
		secret, err := m.GetGroupSecret(ctx, s, x, actAs, p)
		if err != nil {
			return nil, 0, err
		}
		return secret, ratchet, nil
	}
	return nil, 0, fmt.Errorf("no groups give iden=%v access to secret=%v", actAs.GetID(), *hos)
}
