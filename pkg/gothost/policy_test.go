package gothost

import (
	"regexp"
	"testing"

	"github.com/inet256/inet256/pkg/inet256"
	"github.com/stretchr/testify/require"
)

func TestParsePolicy(t *testing.T) {
	tcs := []Policy{
		{
			Rules: []Rule{
				{Allow: true, Subject: NewNamed("groupA"), Verb: OpLook, Object: regexp.MustCompile(".*")},
				{Allow: true, Subject: Anyone(), Verb: OpLook, Object: regexp.MustCompile("abc")},
				{Allow: true, Subject: NewPeer(inet256.Addr{}), Verb: OpTouch, Object: regexp.MustCompile("123")},
			},
		},
	}
	for _, x := range tcs {
		data := MarshalPolicy(x)
		y, err := ParsePolicy(data)
		require.NoError(t, err)
		require.Equal(t, x, *y)
	}
}
