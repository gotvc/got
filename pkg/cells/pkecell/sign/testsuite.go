package sign

import (
	mrand "math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScheme[Priv, Pub any](t *testing.T, scheme Scheme[Priv, Pub]) {
	generate := func(i int) (Priv, Pub) {
		rng := mrand.New(mrand.NewSource(int64(i)))
		priv, pub, err := scheme.Generate(rng)
		require.NoError(t, err)
		return priv, pub
	}
	t.Run("Generate", func(t *testing.T) {
		rng := mrand.New(mrand.NewSource(0))
		priv, pub, err := scheme.Generate(rng)
		require.NoError(t, err)
		require.NotNil(t, priv)
		require.NotNil(t, pub)
	})
	t.Run("MarshalParsePublic", func(t *testing.T) {
		_, pub := generate(0)
		data := scheme.MarshalPublic(pub)
		require.Len(t, data, scheme.PublicKeySize)
		pub2, err := scheme.ParsePublic(data)
		require.NoError(t, err)
		require.Equal(t, pub, pub2)
	})
}
