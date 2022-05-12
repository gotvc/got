package goturl

import (
	"testing"

	"github.com/inet256/inet256/pkg/inet256"
	"github.com/stretchr/testify/require"
)

func TestSpaceURL(t *testing.T) {
	urls := []SpaceURL{
		{
			Protocol:    ProtocolNative,
			Host:        inet256.Addr{}.String(),
			SpacePrefix: "space-prefix",
		},
		{
			Protocol:    ProtocolQUIC,
			Host:        inet256.Addr{}.String() + "@" + "example.com:443",
			SpacePrefix: "space-prefix",
		},
		{
			Protocol:    ProtocolGRPC,
			Host:        "example.com:443",
			SpacePrefix: "space-prefix",
		},
	}
	for _, x := range urls {
		s := x.String()
		t.Log(s)
		y, err := ParseSpaceURL(s)
		require.NoError(t, err)
		require.Equal(t, x, *y)
	}
}
