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
		N = 1e7

		minSize = 64
		avgSize = 1 << 14
		maxSize = 1 << 20
	)

	t.Run("Random", func(t *testing.T) {
		t.Parallel()
		r := io.LimitReader(randReader{}, N)
		sizes := testChunker(t, r, minSize, maxSize, func(ch ChunkHandler) Chunker {
			key := [32]byte{}
			return NewContentDefined(minSize, avgSize, maxSize, &key, ch)
		})
		mu := mean(sizes)
		sigma := stddev(sizes, mu)
		total := sum(sizes)
		count := len(sizes)
		t.Log("mu:", mu, "sigma:", sigma, "sum", total, "count:", count)

		// all the data should have been chunked
		require.Equal(t, int(N), total)
		// average size should be close to what we expect
		withinTolerance(t, mu, avgSize, 0.05)
	})

	t.Run("Zero", func(t *testing.T) {
		t.Parallel()
		r := io.LimitReader(zeroReader{}, N)
		sizes := testChunker(t, r, minSize, maxSize, func(ch ChunkHandler) Chunker {
			key := [32]byte{}
			return NewContentDefined(64, avgSize, maxSize, &key, ch)
		})
		mu := mean(sizes)
		sigma := stddev(sizes, mu)
		total := sum(sizes)
		count := len(sizes)
		t.Log("mu:", mu, "sigma:", sigma, "sum", total, "count:", count)

		// all the data should have been chunked
		require.Equal(t, int(N), total)
		// average size should be much bigger than min size
		require.Greater(t, mu, 64*100)
	})
}

func BenchmarkContentDefined(b *testing.B) {
	const N = 100e6
	var totalSize int
	var count int
	c := NewContentDefined(64, 1<<13, 1<<20, new([32]byte), func(data []byte) error {
		totalSize += len(data)
		count++
		return nil
	})
	b.SetBytes(N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		totalSize = 0
		count = 0
		rng := mrand.New(mrand.NewSource(0))
		n, err := io.CopyN(c, rng, N)
		if err != nil {
			b.Error(err)
		}
		if err := c.Flush(); err != nil {
			b.Error(err)
		}
		if n != N {
			b.Errorf("%d != %d", int(N), n)
		}
		if totalSize != N {
			b.Errorf("%d != %d", int(N), totalSize)
		}
		b.ReportMetric(float64(count), "chunks")
	}
}

func testChunker(t *testing.T, r io.Reader, minSize, maxSize int, newChunker func(func(data []byte) error) Chunker) (sizes []int) {
	c := newChunker(func(data []byte) error {
		require.LessOrEqual(t, len(data), maxSize)
		require.GreaterOrEqual(t, len(data), minSize)
		sizes = append(sizes, len(data))
		return nil
	})
	// copy in some data
	_, err := io.Copy(c, r)
	require.NoError(t, err)
	// close
	require.NoError(t, c.Flush())
	return sizes
}

func withinTolerance(t *testing.T, x int, target int, tol float64) {
	ok := math.Abs(float64(x)-float64(target)) < float64(target)*tol
	require.True(t, ok, "%d not in target (%d +/- %f) == (%f, %f)", x, target, tol, float64(target)*(1-tol), float64(target)*(1+tol))
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

func mean(xs []int) int {
	var sum int
	for _, x := range xs {
		sum += x
	}
	return sum / len(xs)
}

func stddev(xs []int, mean int) float64 {
	var variance float64
	for _, x := range xs {
		variance += math.Abs(float64(x) - float64(mean))
	}
	variance /= float64(len(xs))

	return math.Sqrt(variance)
}

func sum(xs []int) (ret int) {
	for _, x := range xs {
		ret += x
	}
	return ret
}
