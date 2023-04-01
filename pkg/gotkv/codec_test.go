package gotkv

import (
	"strconv"
	"testing"

	"github.com/brendoncarroll/go-state"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
	"github.com/stretchr/testify/require"
)

func TestEncoder(t *testing.T) {
	e := &Encoder{}
	buf := make([]byte, 1<<16)
	ent := kvstreams.Entry{Key: []byte("aa"), Value: []byte("value1")}
	claimedLen := e.EncodedLen(ent)
	n, err := e.Write(buf, ent)
	require.NoError(t, err)
	require.Equal(t, claimedLen, n)
	t.Log("encoder wrote", n, "bytes")

	expected := []byte{9,
		// key
		0, 0,
		// value
		6, 'v', 'a', 'l', 'u', 'e', '1',
	}
	require.Equal(t, expected, buf[:n])

	n2, err := e.Write(buf[n:], kvstreams.Entry{
		Key:   []byte("ab"),
		Value: []byte("value2"),
	})
	require.NoError(t, err)

	expected2 := []byte{10,
		// key
		1, 1, 'b',
		// value
		6, 'v', 'a', 'l', 'u', 'e', '2',
	}
	require.Equal(t, expected2, buf[n:n+n2])
}

func TestEncodeDecode(t *testing.T) {
	buf := make([]byte, 1<<12)
	makeKey := func(i int) []byte { return []byte(strconv.Itoa(i)) }
	makeValue := func(i int) []byte { return []byte("value_" + strconv.Itoa(i)) }

	enc := Encoder{}
	var nwritten int
	for i := 0; i < 10; i++ {
		ent := kvstreams.Entry{
			Key:   makeKey(i),
			Value: makeValue(i),
		}
		claimedLen := enc.EncodedLen(ent)
		n, err := enc.Write(buf[nwritten:], ent)
		require.NoError(t, err)
		require.Equal(t, claimedLen, n)
		t.Logf("encoded entry %v in %d bytes", ent, n)
		nwritten += n
	}

	dec := Decoder{}
	span := state.TotalSpan[Entry]().WithLowerIncl(Entry{Key: makeKey(0)})
	dec.Reset(Index{Span: span})
	var nread int
	var ent kvstreams.Entry
	for i := 0; i < 10; i++ {
		n, err := dec.Read(buf[nread:], &ent)
		require.NoError(t, err)
		nread += n
		require.Equal(t, string(makeKey(i)), string(ent.Key), "on %d", i)
		require.Equal(t, string(makeValue(i)), string(ent.Value), "on %d", i)
	}
	require.Equal(t, nwritten, nread)
}
