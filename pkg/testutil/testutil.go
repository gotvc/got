package testutil

import (
	"bufio"
	"fmt"
	"io"
	"math"
	mrand "math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func StreamsEqual(t testing.TB, expected, actual io.Reader) {
	br1 := bufio.NewReaderSize(expected, 1<<20)
	br2 := bufio.NewReaderSize(actual, 1<<20)
	for i := 0; ; i++ {
		b1, err1 := br1.ReadByte()
		if err1 != nil && err1 != io.EOF {
			require.NoError(t, err1)
		}
		b2, err2 := br2.ReadByte()
		if err2 != nil && err2 != io.EOF {
			require.NoError(t, err1)
		}
		if err1 != err2 {
			if err1 == io.EOF {
				t.Fatalf("stream is longer than expected. len=%d", i)
			} else if err2 == io.EOF {
				t.Fatalf("stream is shorter than expected. len=%d", i)
			}
		}
		// Since require.Equal uses reflection, this additional check
		// is repsponsible for a ~40x performance improvement.
		if b1 != b2 {
			require.Equal(t, b1, b2, "bytes unequal at %d: %x vs %x", i, b1, b2)
		}
		if err1 == io.EOF {
			break
		}
	}
}

func RandomStream(seed int, size int64) io.Reader {
	return io.LimitReader(mrand.New(mrand.NewSource(int64(seed))), size)
}

func RandomFiles(seed int, numFiles int, sizeMean, sizeSig float64, fn func(string, io.Reader)) {
	fmtStr := "%0" + strconv.Itoa(int(math.Ceil(math.Log10(float64(numFiles))))) + "d"
	for i := 0; i < numFiles; i++ {
		rng := mrand.New(mrand.NewSource(int64(i)))
		size := int64(math.Round(rng.NormFloat64()*sizeSig + sizeMean))
		p := fmt.Sprintf(fmtStr, i)
		fn(p, RandomStream(i, size))
	}
}
