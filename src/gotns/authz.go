package gotns

import (
	"context"
	"encoding/binary"
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
	// Volume is the volume that the obligation is for.
	// Accessing the data in this volume requires the seed encrypted in the obligation value.
	Volume blobcache.OID
	// KEMHash is the hash of the KEM public key that the obligation is for.
	KEMHash [32]byte
	// Nonce is increased each time the volume keys change.
	Nonce uint64

	// The seed can be decrypted with the corresponding private key for the KEMHash.
	// The seed will decrypt to a 64 byte seed, which is used for the volume's Signing Key.
	// The hash of the seed will be used for the symmetric cipher.
	EncryptedSeed []byte
	// RuleIDs is the rule that required the obligation.
	RuleIDs []RuleID
}

func (o *Obligation) Key(out []byte) []byte {
	out = append(out, o.Volume[:]...)
	out = append(out, o.KEMHash[:]...)
	out = binary.BigEndian.AppendUint64(out, o.Nonce)
	return out
}

func (o *Obligation) Value(out []byte) []byte {
	out = sbe.AppendLP(out, o.EncryptedSeed)
	out = binary.AppendUvarint(out, uint64(len(o.RuleIDs)))
	for _, ruleID := range o.RuleIDs {
		out = append(out, ruleID[:]...)
	}
	return out
}

func ParseObligation(key []byte, value []byte) (*Obligation, error) {
	// key
	if len(key) < blobcache.OIDSize+32+8 {
		return nil, fmt.Errorf("key too short")
	}
	volID := blobcache.OID(key[:blobcache.OIDSize])
	kemHash := [32]byte(key[blobcache.OIDSize : blobcache.OIDSize+32])
	nonce := binary.BigEndian.Uint64(key[blobcache.OIDSize+32:])

	// value
	encryptedSeed, value, err := sbe.ReadLP(value)
	if err != nil {
		return nil, err
	}
	ruleIDsLen, value, err := sbe.ReadUVarint(value[len(encryptedSeed):])
	if err != nil {
		return nil, err
	}
	var ruleIDs []RuleID
	for range ruleIDsLen {
		var ruleID RuleID
		copy(ruleID[:], value)
		value = value[32:]
		ruleIDs = append(ruleIDs, ruleID)
	}
	return &Obligation{
		Volume:  volID,
		KEMHash: kemHash,
		Nonce:   nonce,

		EncryptedSeed: encryptedSeed,
		RuleIDs:       ruleIDs,
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

// EnsureObligations ensures that obligations for the entry are satisfied.
func (m *Machine) EnsureObligations(ctx context.Context, s stores.Reading, state State, ent Entry, secret *[32]byte) (bool, error) {
	if err := m.ForEachRule(ctx, s, state, func(rule gotnsop.Rule) error {
		if rule.ObjectType != gotnsop.ObjectType_BRANCH || !rule.Names.MatchString(ent.Name) {
			return nil
		}
		return nil
	}); err != nil {
		return false, err
	}
	return false, nil
}

type CID = blobcache.CID

func (m *Machine) addInitialRules(ctx context.Context, s stores.RW, state State, adminGroupName string) (*State, error) {
	for _, rule := range []gotnsop.Rule{
		{
			Subject:    adminGroupName,
			Verb:       gotnsop.Verb_ADMIN,
			ObjectType: gotnsop.ObjectType_GROUP,
			Names:      regexp.MustCompile(".*"),
		},
		{
			Subject:    adminGroupName,
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
