package gotnsop

import (
	"regexp"
	"strconv"
	"testing"

	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.inet256.org/inet256/src/inet256"
)

func TestMarshalOp(t *testing.T) {
	pubKey, _ := testutil.NewSigner(t, 0)
	kemPub, _ := testutil.NewKEM(t, 0)
	tc := []Op{
		&CreateGroup{Group: Group{Name: "test", KEM: kemPub, Owners: nil, LeafKEMs: map[inet256.ID][]byte{}}},
		&CreateLeaf{Leaf: NewLeaf(pubKey, kemPub)},

		&AddLeaf{Group: "a", LeafID: inet256.ID{}},
		&RemoveLeaf{Group: "a", ID: inet256.ID{}},
		&AddMember{Group: "a", Member: "b", EncryptedKEM: []byte{}},
		&RemoveMember{Group: "a", Member: "b"},
		&AddRule{Rule: Rule{Subject: "sub", Verb: "verb", ObjectType: ObjectType_GROUP, Names: regexp.MustCompile(".*")}},
		&DropRule{RuleID: RuleID{}},
		&PutBranchEntry{Entry: BranchEntry{Name: "test", Aux: []byte{}}},
		&DeleteBranchEntry{Name: "test"},
	}

	for i, tc := range tc {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			x := tc
			data := AppendOp(nil, x)
			y, rest, err := ReadOp(data)
			require.NoError(t, err)
			require.Len(t, rest, 0)
			require.Equal(t, x, y)
		})
	}
}

func TestMarshalGroup(t *testing.T) {
	kemPub, _ := testutil.NewKEM(t, 0)
	group := Group{Name: "test", KEM: kemPub, Owners: nil, LeafKEMs: map[inet256.ID][]byte{}}
	k := group.Key(nil)
	val := group.Value(nil)
	group2, err := ParseGroup(k, val)
	require.NoError(t, err)
	require.Equal(t, group, *group2)
}
