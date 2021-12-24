package testutil

import (
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func StreamsEqual(t testing.TB, a, b io.Reader) {
	const bufSize = 1 << 15
	buf1 := make([]byte, bufSize)
	buf2 := make([]byte, bufSize)
	for offset := 0; ; offset += bufSize {
		n1, err1 := io.ReadFull(a, buf1)
		if err1 != io.ErrUnexpectedEOF {
			require.NoError(t, err1)
		}
		n2, err2 := io.ReadFull(b, buf2)
		if err2 != io.ErrUnexpectedEOF {
			require.NoError(t, err2)
		}
		require.Equal(t, err1, err2, "different errors at byte %d", offset)
		require.Equal(t, n1, n2, "streams unequal lengths at byte %d", offset)
		for j := 0; j < n1 && j < n2; j++ {
			b1, b2 := buf1[j], buf2[j]
			// Since require.Equal uses reflection, this additional check
			// is repsponsible for a ~40x performance improvement.
			if b1 != b2 {
				require.Equal(t, b1, b2, "bytes unequal at %d: %x vs %x", offset+j, b1, b2)
			}
		}
		if err1 == io.ErrUnexpectedEOF || err2 == io.ErrUnexpectedEOF {
			break
		}
	}
}

func RandomStream(seed, size int64) io.Reader {
	return io.LimitReader(rand.New(rand.NewSource(seed)), size)
}
