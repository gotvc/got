package chunking

import (
	"io"
	"math"
	mrand "math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContentDefined(t *testing.T) {
	t.Parallel()
	const (
		N       = 1e7
		avgSize = 1 << 13
		maxSize = 1 << 20
	)

	t.Run("Random", func(t *testing.T) {
		t.Parallel()
		r := io.LimitReader(randReader{}, N)
		count, size := testChunker(t, func(ch ChunkHandler) Chunker {
			poly := DerivePolynomial(nil)
			return NewContentDefined(64, avgSize, maxSize, poly, ch)
		}, r, maxSize)
		// average size should be close to what we expect
		t.Log("size:", size, "count:", count, "avg:", size/count)
		withinTolerance(t, size/count, avgSize, 0.05)
		// all the data should have been chunked
		require.Equal(t, int(N), size)
	})

	t.Run("Zero", func(t *testing.T) {
		t.Parallel()
		r := io.LimitReader(zeroReader{}, N)
		count, size := testChunker(t, func(ch ChunkHandler) Chunker {
			poly := DerivePolynomial(nil)
			return NewContentDefined(64, avgSize, maxSize, poly, ch)
		}, r, maxSize)
		// average size should be much bigger than min size
		t.Log("size:", size, "count:", count, "avg:", size/count)
		require.Greater(t, size/count, 64*100)

		// all the data should have been chunked
		require.Equal(t, int(N), size)

	})
}

func testChunker(t *testing.T, newChunker func(func(data []byte) error) Chunker, r io.Reader, maxSize int) (count, size int) {
	c := newChunker(func(data []byte) error {
		count++
		size += len(data)
		require.LessOrEqual(t, len(data), maxSize)
		return nil
	})
	// copy in some data
	_, err := io.Copy(c, r)
	require.NoError(t, err)
	// close
	require.NoError(t, c.Flush())
	return count, size
}

func withinTolerance(t *testing.T, x int, target int, tol float64) {
	ok := math.Abs(float64(x)-float64(target)) < float64(target)*tol
	require.True(t, ok, "%d not within +/- %f of target %d", x, tol, target)
}

type randReader struct{}

func (r randReader) Read(p []byte) (n int, err error) {
	return mrand.Read(p)
}

type zeroReader struct{ c byte }

func (r zeroReader) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = r.c
	}
	return len(p), nil
}
