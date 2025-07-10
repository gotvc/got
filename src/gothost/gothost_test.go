package gothost

import (
	"testing"

	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/internal/cells"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.inet256.org/inet256/pkg/inet256"
)

func TestConfigureDefaults(t *testing.T) {
	ctx := testutil.Context(t)
	space := branches.NewMem(stores.NewMem, cells.NewMem)
	e := NewHostEngine(space)
	require.NoError(t, e.Initialize(ctx))
	require.NoError(t, e.Modify(ctx, ConfigureDefaults([]PeerID{newID(t, 0)})))

	s2 := e.Open(newID(t, 0))
	_, err := s2.Create(ctx, "test", branches.NewConfig(false))
	require.NoError(t, err)
}

func newID(t testing.TB, i int) (ret inet256.Addr) {
	ret[0] = uint8(i)
	return ret
}
