package gdat

import (
	"context"
	"fmt"
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/stretchr/testify/require"
)

func TestMarshalUnmarshal(t *testing.T) {
	ctx := context.Background()
	s := cadata.NewMem(cadata.DefaultHash, cadata.DefaultMaxSize)
	op := NewOperator()
	x, err := op.Post(ctx, s, []byte("test data"))
	require.NoError(t, err)

	ser := MarshalRef(*x)
	y, err := ParseRef(ser)
	require.NoError(t, err)
	require.Equal(t, x, y)
}

func TestRefString(t *testing.T) {
	x := Ref{}
	actual := fmt.Sprintf("%v", x)
	expected := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA#AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	require.Equal(t, expected, actual)
}
