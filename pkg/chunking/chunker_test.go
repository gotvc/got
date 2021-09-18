package chunking

import (
	"io"
	"math"
	mrand "math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChunker(t *testing.T) {
	t.Parallel()
	const N = 1e7
	avgSize := 1 << 13
	maxSize := 1 << 20

	var count int
	var size int
	poly := DerivePolynomial(nil)
	c := NewContentDefined(64, avgSize, maxSize, poly, func(data []byte) error {
		count++
		size += len(data)
		require.LessOrEqual(t, len(data), maxSize)
		return nil
	})
	// copy in some data
	_, err := io.Copy(c, io.LimitReader(randReader{}, N))
	require.NoError(t, err)
	// close
	require.NoError(t, c.Flush())

	// average size should be close to what we expect
	t.Log("size:", size, "count:", count, "avg:", size/count)
	withinTolerance(t, size/count, avgSize, 0.05)

	// all the data should have been chunked
	require.Equal(t, int(N), size)
}

func withinTolerance(t *testing.T, x int, target int, tol float64) {
	ok := math.Abs(float64(x)-float64(target)) < float64(target)*tol
	require.True(t, ok)
}

type randReader struct{}

func (r randReader) Read(p []byte) (n int, err error) {
	return mrand.Read(p)
}
