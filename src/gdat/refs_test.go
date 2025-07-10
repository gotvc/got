package gdat

import (
	"fmt"
	"testing"

	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/state/cadata"
)

func TestMarshalUnmarshal(t *testing.T) {
	ctx := testutil.Context(t)
	s := cadata.NewMem(cadata.DefaultHash, cadata.DefaultMaxSize)
	ag := NewMachine()
	x, err := ag.Post(ctx, s, []byte("test data"))
	require.NoError(t, err)

	ser := AppendRef(nil, *x)
	y, err := ParseRef(ser)
	require.NoError(t, err)
	require.Equal(t, *x, y)
}

func TestRefString(t *testing.T) {
	x := Ref{}
	actual := fmt.Sprintf("%v", x)
	expected := "-------------------------------------------#-------------------------------------------"
	require.Equal(t, expected, actual)
}
