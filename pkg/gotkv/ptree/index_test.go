package ptree

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/state"
	"go.brendoncarroll.net/state/cadata"
)

func TestIndex(t *testing.T) {
	idx := Index[Entry, cadata.ID]{
		Ref:       cadata.DefaultHash([]byte("hello")),
		IsNatural: true,
		Span: state.TotalSpan[Entry]().
			WithLowerIncl(Entry{Key: []byte("aaa first key")}).
			WithUpperIncl(Entry{Key: []byte("zzz last key")}),
	}

	m := metaIndex(idx)
	idx2 := flattenIndex(m)
	require.Equal(t, idx, idx2)
}
