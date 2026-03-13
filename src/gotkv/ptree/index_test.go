package ptree

import (
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/state"
)

func TestIndex(t *testing.T) {
	idx := Index[Entry, blobcache.CID]{
		Ref:       stores.Hash([]byte("hello")),
		IsNatural: true,
		Span: state.TotalSpan[Entry]().
			WithLowerIncl(Entry{Key: []byte("aaa first key")}).
			WithUpperIncl(Entry{Key: []byte("zzz last key")}),
	}

	m := metaIndex(idx)
	idx2 := flattenIndex(m)
	require.Equal(t, idx, idx2)
}
