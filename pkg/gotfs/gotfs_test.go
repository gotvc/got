package gotfs

import (
	"encoding/binary"
	"encoding/hex"
	"io"
	mrand "math/rand"
	"testing"

	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/testutil"
	"go.brendoncarroll.net/state/cadata"
)

func BenchmarkWrite(b *testing.B) {
	s := cadata.NewVoid(gdat.Hash, DefaultMaxBlobSize)
	ag := NewMachine()

	b.Run("1-1GB", func(b *testing.B) {
		ctx := testutil.Context(b)
		const size = int64(1e9)
		b.SetBytes(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rng := mrand.New(mrand.NewSource(0))
			bu := ag.NewBuilder(ctx, s, s)
			if err := bu.BeginFile("", 0); err != nil {
				b.Fatal(err)
			}
			if _, err := io.CopyN(bu, rng, size); err != nil {
				b.Fatal(err)
			}
			root, err := bu.Finish()
			if err != nil {
				b.Fatal(err)
			}
			b.Log("root:", root)
		}
	})
	b.Run("10k-10B", func(b *testing.B) {
		ctx := testutil.Context(b)
		const numFiles = 10e3
		const size = 10

		b.SetBytes(numFiles * size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rng := mrand.New(mrand.NewSource(0))
			bu := ag.NewBuilder(ctx, s, s)
			if err := bu.Mkdir("", 0); err != nil {
				b.Fatal(err)
			}
			for j := 0; j < numFiles; j++ {
				var buf [8]byte
				binary.BigEndian.PutUint64(buf[:], uint64(j))
				if err := bu.BeginFile(hex.EncodeToString(buf[:]), 0); err != nil {
					b.Fatal(err)
				}
				if _, err := io.CopyN(bu, rng, size); err != nil {
					b.Fatal(err)
				}
			}
			_, err := bu.Finish()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
