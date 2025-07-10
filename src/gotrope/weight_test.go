package gotrope

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWeightAdd(t *testing.T) {
	tcs := []struct {
		L, R, Y Weight
	}{
		{L: nil, R: Weight{1}, Y: Weight{1}},
		{L: Weight{0}, R: Weight{1}, Y: Weight{1}},
		{L: Weight{2}, R: Weight{3}, Y: Weight{5}},
	}
	for _, tc := range tcs {
		var y Weight
		y.Add(tc.L, tc.R)
		require.Equal(t, tc.Y, y)
	}
}
