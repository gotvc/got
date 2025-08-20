package gotns

import (
	"context"
	"encoding/binary"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/streams"
)

type Root struct {
	// Groups maps group names to group information.
	// Group information holds group's Owner list, and shared KEM key for the group
	// to receive messages.
	Groups gotkv.Root
	// Leaves hold relationships between groups and primitive identity elements.
	// The first part of the key is the group name, and the last 32 bytes are the Leaf's ID.
	// The value is the public signing key for the leaf, and a signed KEM key for the leaf
	// to recieve messages
	Leaves gotkv.Root
	// Memberships holds relationships between groups and other groups.
	// The key is the containing group + the member group + len(member group name)
	// The value is a KEM ciphertext sent by the containing group owner to the member.
	// The ciphertext contains the containing group's KEM private key encrypted with the member's KEM public key.
	Memberships gotkv.Root
	// Rules holds rules for the namespace, granting look or touch access to branches.
	Rules gotkv.Root
	// Branches holds the actual branch entries themselves.
	// Branch entries contain a volume OID, and a set of rights (which is for Blobcache)
	// They also contain a set of DEK hashes for the DEKs that should be used to encrypt the volume
	// and a map from Hash(KEM public key) to the KEM ciphertext for the DEK encrypted with the KEM public key.
	Branches gotkv.Root
}

func (r Root) Marshal(out []byte) []byte {
	const versionTag = 0
	out = append(out, versionTag)
	out = appendLP(out, r.Groups.Marshal())
	out = appendLP(out, r.Memberships.Marshal())
	out = appendLP(out, r.Rules.Marshal())
	out = appendLP(out, r.Branches.Marshal())
	return out
}

func (r *Root) Unmarshal(data []byte) error {
	if len(data) < 1 {
		return fmt.Errorf("too short to contain version tag")
	}
	switch data[0] {
	case 0:
	default:
		return fmt.Errorf("unknown version tag: %d", data[0])
	}
	data = data[1:]

	for _, dst := range []*gotkv.Root{&r.Groups, &r.Memberships, &r.Rules, &r.Branches} {
		kvrData, n, err := readLP(data)
		if err != nil {
			return err
		}
		if err := dst.Unmarshal(kvrData); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

func parseRoot(x []byte) (*Root, error) {
	if len(x) == 0 {
		return nil, nil
	}
	var root Root
	if err := root.Unmarshal(x); err != nil {
		return nil, err
	}
	return &root, nil
}

type Machine struct {
	gotkv gotkv.Machine
}

func New() Machine {
	return Machine{
		gotkv: gotkv.NewMachine(1<<14, 1<<20),
	}
}

func (m *Machine) New(ctx context.Context, s stores.RW) (*Root, error) {
	var r Root
	for _, dst := range []*gotkv.Root{&r.Groups, &r.Memberships, &r.Rules, &r.Branches} {
		kvr, err := m.gotkv.NewEmpty(ctx, s)
		if err != nil {
			return nil, err
		}
		*dst = *kvr
	}
	return &r, nil
}

func (m *Machine) Validate(ctx context.Context, src stores.Reading, prev, next *Root) error {
	if prev == nil {
		// if there is no previous root, then just make sure the next root is valid.
		for _, kvr := range []gotkv.Root{next.Groups, next.Memberships, next.Rules, next.Branches} {
			if kvr.Ref.CID.IsZero() {
				return fmt.Errorf("gotns: one of the roots is uninitialized")
			}
		}
		return nil
	}
	if next == nil {
		return fmt.Errorf("gotns: the next root must not be nil")
	}

	// TODO: first validate auth operations, ensure that all the differences are signed.
	return nil
}

func (m *Machine) AddMember(ctx context.Context, s stores.RW, root Root, name string, member string) (*Root, error) {
	// TODO: Need to make sure group exists first.
	memRoot, err := m.gotkv.Mutate(ctx, s, root.Memberships,
		addMember(Membership{
			Group:  name,
			Member: member,
		}),
	)
	if err != nil {
		return nil, err
	}
	root.Memberships = *memRoot
	return &root, nil
}

func (m *Machine) RemoveMember(ctx context.Context, s stores.RW, root Root, group string, member string) (*Root, error) {
	// TODO: Need to make sure group exists first.
	memRoot, err := m.gotkv.Mutate(ctx, s, root.Memberships,
		rmMember(group, member),
	)
	if err != nil {
		return nil, err
	}
	root.Memberships = *memRoot
	return &root, nil
}
func (m *Machine) GetGroup(ctx context.Context, s stores.Reading, root Root, name string) (*Group, error) {
	return nil, nil
}

// NewEntry creates a new entry with the provided information
// and produces KEMs for each group with access to the entry.
func (m *Machine) NewEntry(ctx context.Context, name string, rights blobcache.ActionSet, volume blobcache.OID, secret *[32]byte) (Entry, error) {
	entry := Entry{
		Name:   name,
		Rights: rights,
		Volume: volume,
	}
	return entry, nil
}

type Entry struct {
	Name   string
	Volume blobcache.OID
	Rights blobcache.ActionSet

	// DEK hashes are the hashes of the DEKs for this entry.
	// We have 2 because the actual contents of the volume are not consistent
	// with the namespace.
	// Rotating the key would require adding it in the namespace first
	// and then re-encrypting the volume root with the new key.
	DEKHashes [][32]byte

	// KEMCtexts contains DEKs encrypted with KEMs for each group with access to the entry.
	KEMCTexts map[[32]byte][]byte
}

func (e Entry) Key(buf []byte) []byte {
	buf = append(buf, e.Name...)
	return buf
}

func (e Entry) Value(buf []byte) []byte {
	buf = append(buf, e.Volume[:]...)
	buf = binary.BigEndian.AppendUint64(buf, uint64(e.Rights))
	for pubHash, ctext := range e.KEMCTexts {
		// each element is concatenated keyhash and ctext, and then appended.
		buf = appendLP(buf, append(pubHash[:], ctext...))
	}
	return buf
}

func ParseEntry(key, value []byte) (Entry, error) {
	var entry Entry
	entry.Name = string(key)

	if len(value) < 16+8 {
		return Entry{}, fmt.Errorf("entry value too short")
	}
	entry.Volume = blobcache.OID(value[:16])
	entry.Rights = blobcache.ActionSet(binary.BigEndian.Uint64(value[16:24]))

	return entry, nil
}

func (m *Machine) GetEntry(ctx context.Context, s stores.Reading, root Root, name []byte) (*Entry, error) {
	val, err := m.gotkv.Get(ctx, s, root.Branches, name)
	if err != nil {
		if gotkv.IsErrKeyNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	entry, err := ParseEntry(name, val)
	if err != nil {
		return nil, err
	}
	return &entry, nil
}

func (m *Machine) PutEntry(ctx context.Context, s stores.RW, root Root, entry Entry) (*Root, error) {
	mut := PutEntry(entry)
	entsRoot, err := m.gotkv.Mutate(ctx, s, root.Branches, mut)
	if err != nil {
		return nil, err
	}
	root.Branches = *entsRoot
	return &root, nil
}

func (m *Machine) DeleteEntry(ctx context.Context, s stores.RW, root Root, name string) (*Root, error) {
	entsRoot, err := m.gotkv.Delete(ctx, s, root.Branches, []byte(name))
	if err != nil {
		return nil, err
	}
	root.Branches = *entsRoot
	return &root, nil
}

func PutEntry(entry Entry) gotkv.Mutation {
	k := entry.Key(nil)
	return gotkv.Mutation{
		Span: gotkv.SingleKeySpan(k),
		Entries: []gotkv.Entry{
			{
				Key:   entry.Key(nil),
				Value: entry.Value(nil),
			},
		},
	}
}

func (m *Machine) ListEntries(ctx context.Context, s stores.Reading, root Root, limit int) ([]Entry, error) {
	span := gotkv.PrefixSpan([]byte(""))
	it := m.gotkv.NewIterator(s, root.Branches, span)
	var ents []Entry
	for {
		ent, err := streams.Next(ctx, it)
		if err != nil {
			if streams.IsEOS(err) {
				break
			}
			return nil, err
		}

		entry, err := ParseEntry(ent.Key, ent.Value)
		if err != nil {
			return nil, err
		}
		ents = append(ents, entry)
		if limit > 0 && len(ents) >= limit {
			break
		}
	}
	return ents, nil
}

// appendLP appends a length-prefixed byte slice to out.
func appendLP(out []byte, data []byte) []byte {
	out = binary.AppendUvarint(out, uint64(len(data)))
	return append(out, data...)
}

func readLP(data []byte) ([]byte, int, error) {
	dataLen, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, 0, fmt.Errorf("invalid length-prefixed data")
	}
	if len(data) < n+int(dataLen) {
		return nil, 0, fmt.Errorf("length-prefixed data too short")
	}
	return data[n : n+int(dataLen)], n + int(dataLen), nil
}
