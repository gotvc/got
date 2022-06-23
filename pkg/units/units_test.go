package units

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSIPrefix(t *testing.T) {
	_, p := SIPrefix(123)
	assert.Equal(t, "", p)

	_, p = SIPrefix(1.23e3)
	assert.Equal(t, "K", p)

	_, p = SIPrefix(10_000)
	assert.Equal(t, "K", p)

	_, p = SIPrefix(10_000_000)
	assert.Equal(t, "M", p)

	_, p = SIPrefix(1e9)
	assert.Equal(t, "G", p)
}
