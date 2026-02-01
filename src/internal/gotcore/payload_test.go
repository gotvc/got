package gotcore

import (
	"fmt"
	"testing"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotvc"
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
	root, err := gfs.NewEmpty(ctx, s, 0o755)
	require.NoError(t, err)
	tcs := []Snap{
		{
			N:         1,
			CreatedAt: tai64.Now().TAI64(),
			Parents: []gotvc.Ref{
				{},
				{},
				{},
			},
			Creator: inet256.ID{},
			Payload: Payload{
				Root: *root,
				Aux:  []byte{},
			},
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			x := tc
			data := x.Marshal(nil)
			var y Snap
			require.NoError(t, y.Unmarshal(data, ParsePayload))
			require.Equal(t, x, y)
		})
	}
}
