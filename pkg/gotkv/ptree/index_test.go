package ptree

import (
	"testing"

	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/stretchr/testify/require"
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
