package gotvc

import (
	"fmt"
	"testing"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/tai64"
	"go.inet256.org/inet256/src/inet256"
)

func TestMarshalSnapshot(t *testing.T) {
	ctx := testutil.Context(t)
	s := stores.NewMem()
	gfs := gotfs.NewMachine()
	root, err := gfs.NewEmpty(ctx, s)
	require.NoError(t, err)
	tcs := []Snap{
		{
			N: 1,
			Parents: []Ref{
				{},
				{},
				{},
			},
			Root:      *root,
			CreatedAt: tai64.Now().TAI64(),
			Creator:   inet256.ID{},
			Aux:       []byte{},
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			x := tc
			data := x.Marshal(nil)
			var y Snap
			require.NoError(t, y.Unmarshal(data))
			require.Equal(t, x, y)
		})
	}
}
