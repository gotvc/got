package gotns

import (
	"regexp"
	"strconv"
	"testing"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/simplens"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.inet256.org/inet256/src/inet256"
)

func TestInit(t *testing.T) {
	ctx := testutil.Context(t)
	bc := bclocal.NewTestService(t)
	nsc := simplens.Client{Service: bc}
	volh, err := nsc.CreateAt(ctx, blobcache.Handle{}, "test", blobcache.DefaultLocalSpec())
	require.NoError(t, err)

	gnsc := Client{Blobcache: bc, Machine: New()}
	err = gnsc.Init(ctx, *volh)
	require.NoError(t, err)
}

func TestMarshalOp(t *testing.T) {
	tc := []Op{
		// &Op_CreateGroup{Group: Group{Name: "test"}},
		// &Op_AddLeaf{},
		&Op_DropLeaf{LeafID: inet256.ID{}},
		&Op_AddMember{Group: "a", Member: "b"},
		&Op_RemoveMember{Group: "a", Member: "b"},
		&Op_AddRule{Rule: Rule{Subject: "sub", Verb: "verb", Object: NewGroupSet(regexp.MustCompile(".*"))}},
		&Op_DropRule{RuleID: CID{}},
		&Op_PutEntry{Entry: Entry{Name: "test", Aux: []byte{}}},
		&Op_DeleteEntry{Name: "test"},
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
