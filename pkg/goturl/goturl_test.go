package goturl

import (
	"testing"

	"github.com/inet256/inet256/pkg/inet256"
	"github.com/stretchr/testify/require"
)

func TestParseURL(t *testing.T) {
	urls := []URL{
		{
			Protocol: ProtocolNative,
			Host:     inet256.Addr{}.String(),
		},
		{
			Protocol: ProtocolQUIC,
			Host:     inet256.Addr{}.String() + "@" + "example.com:443",
		},
		{
			Protocol: ProtocolGRPC,
			Host:     "example.com:443",
		},
	}
	for _, x := range urls {
		s := x.String()
		y, err := ParseURL(s)
		require.NoError(t, err)
		require.Equal(t, x, *y)
	}
}
