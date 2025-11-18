package testutil

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	mrand "math/rand"
	"net"
	"strconv"
	"testing"

	"github.com/cloudflare/circl/kem"
	"github.com/cloudflare/circl/kem/mlkem/mlkem1024"
	"github.com/cloudflare/circl/sign"
	"github.com/cloudflare/circl/sign/mldsa/mldsa87"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

func Context(t testing.TB) context.Context {
	ctx := context.Background()
	l, err := zap.NewDevelopment()
	require.NoError(t, err)
	ctx = logctx.NewContext(ctx, l)
	return ctx
}

func StreamsEqual(t testing.TB, expected, actual io.Reader) {
	t.Helper()
	br1 := bufio.NewReaderSize(expected, 1<<20)
	br2 := bufio.NewReaderSize(actual, 1<<20)
	for i := 0; ; i++ {
		b1, err1 := br1.ReadByte()
		if err1 != nil && err1 != io.EOF {
			require.NoError(t, err1)
		}
		b2, err2 := br2.ReadByte()
		if err2 != nil && err2 != io.EOF {
			require.NoError(t, err2)
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

func PacketConn(t testing.TB) net.PacketConn {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() {
		pc.Close()
	})
	return pc
}

func NewSigner(t *testing.T, i uint64) (sign.PublicKey, sign.PrivateKey) {
	var seed [32]byte
	binary.LittleEndian.PutUint64(seed[:], i)
	pub, priv := mldsa87.Scheme().DeriveKey(seed[:])

	return pub, priv
}

func NewKEM(t *testing.T, i uint64) (kem.PublicKey, kem.PrivateKey) {
	var seed [64]byte
	binary.LittleEndian.PutUint64(seed[:], i)
	pub, priv := mlkem1024.Scheme().DeriveKeyPair(seed[:])
	return pub, priv
}
