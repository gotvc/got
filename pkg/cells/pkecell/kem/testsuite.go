package kem

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
	t.Run("EncapDecap", func(t *testing.T) {
		priv, pub := generate(0)
		var seed [SeedSize]byte
		var shared1, shared2 [SharedSecretSize]byte
		ctext := make([]byte, scheme.CiphertextSize)
		scheme.Encapsulate(pub, &seed, &shared1, ctext)

		err := scheme.Decapsulate(priv, &shared2, ctext)
		require.NoError(t, err)

		require.NotZero(t, shared1)
		require.NotZero(t, shared2)
		require.Equal(t, shared1, shared2)
	})
}
