package gotfs

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"maps"
	mrand "math/rand"
	"slices"
	"testing"

	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/exp/streams"
)

func TestKeys(t *testing.T) {
	type testCase struct {
		Files  map[string]string
		Dirs   map[string]fs.FileMode
		Expect [][]byte
	}
	tcs := []testCase{
		{
			Files: map[string]string{
				"": "",
			},
			Expect: [][]byte{
				makeInfoKey(""),
			},
		},
		{
			Dirs: map[string]fs.FileMode{
				"": 0o755,
			},
			Expect: [][]byte{
				makeInfoKey(""),
			},
		},
		{
			Files: map[string]string{
				"": "hello world",
			},
			Expect: [][]byte{
				makeInfoKey(""),
				makeExtentKey("", len("hello world")),
			},
		},
		{
			Dirs: map[string]fs.FileMode{
				"": 0o755,
			},
			Files: map[string]string{
				"a.txt": "",
				"b.txt": "",
			},
			Expect: [][]byte{
				makeInfoKey(""),
				makeInfoKey("a.txt"),
				makeInfoKey("b.txt"),
			},
		},
		{
			Dirs: map[string]fs.FileMode{
				"":     0o755,
				"dir1": 0o755,
			},
			Files: map[string]string{
				"a.txt":       "",
				"dir1/1a.txt": "",
				"b.txt":       "",
				"e.txt":       "",
			},
			Expect: [][]byte{
				makeInfoKey(""),
				makeInfoKey("a.txt"),
				makeInfoKey("b.txt"),
				makeInfoKey("dir1"),
				makeInfoKey("dir1/1a.txt"),
				makeInfoKey("e.txt"),
			},
		},
	}
	kvmach := gotkv.NewMachine(DefaultMeanBlobSizeMetadata, DefaultMaxBlobSize)
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			ctx, mach, s := setup(t)

			var ps []string
			ps = slices.AppendSeq(ps, maps.Keys(tc.Files))
			ps = slices.AppendSeq(ps, maps.Keys(tc.Dirs))
			slices.Sort(ps)

			// build file system
			b := mach.NewBuilder(ctx, s, s)
			for _, p := range ps {
				if dmode, ok := tc.Dirs[p]; ok {
					require.NoError(t, b.Mkdir(p, dmode))
				}
				if fstr, ok := tc.Files[p]; ok {
					require.NoError(t, b.BeginFile(p, 0o644))
					_, err := b.Write([]byte(fstr))
					require.NoError(t, err)
				}
			}
			root, err := b.Finish()
			require.NoError(t, err)

			// collect all the keys and check that they match
			kvit := kvmach.NewIterator(s, root.ToGotKV(), gotkv.TotalSpan())
			var actual [][]byte
			require.NoError(t, streams.ForEach(ctx, kvit, func(ent gotkv.Entry) error {
				actual = append(actual, bytes.Clone(ent.Key))
				return nil
			}))
			if !assert.Equal(t, tc.Expect, actual) {
				t.Log("EXPECT:")
				for i, k := range tc.Expect {
					t.Logf("%d %q", i, k)
				}
				t.Log("ACTUAL:")
				for i, k := range actual {
					t.Logf("%d %q", i, k)
				}
			}
		})
	}
}

func BenchmarkWrite(b *testing.B) {
	s := stores.NewMem()
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

func makeInfoKey(p string) []byte {
	return newInfoKey(p).Marshal(nil)
}

func makeExtentKey(p string, endAt int) (out []byte) {
	return newExtentKey(p, uint64(endAt)).Marshal(nil)
}
