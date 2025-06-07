package gotrope

import (
	"errors"
	"fmt"
	"testing"

	"github.com/gotvc/got/pkg/gdat"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/state/cadata"
)

const (
	defaultMeanSize = 1 << 12
	defaultMaxSize  = 1 << 16
)

func TestEmpty(t *testing.T) {
	type Ref = cadata.ID
	s := newStore(t)
	b := NewBuilder(s, defaultMeanSize, defaultMaxSize, nil)
	root, err := b.Finish(ctx)
	require.NoError(t, err)
	require.NotNil(t, root)
}

func TestBuildIterate(t *testing.T) {
	s := newStore(t)
	b := newTestBuilder(t, s)

	const N = 10000
	var v []byte
	for i := 0; i < N; i++ {
		v = fmt.Appendf(v[:0], "hello world %d", i)
		require.NoError(t, b.Append(ctx, 0, v))
	}

	root, err := b.Finish(ctx)
	require.NoError(t, err)
	require.NotNil(t, root)
	require.Equal(t, root.Weight, Weight{N})
	t.Log("depth", root.Depth)

	it := NewIterator[Ref](s, *root, TotalSpan())
	var ent Entry
	for i := 0; i < N; i++ {
		err := it.Next(ctx, &ent)
		require.NoError(t, err, i)
		require.Equal(t, Path{uint64(i)}, ent.Path)
	}
	require.ErrorIs(t, it.Next(ctx, &ent), EOS())
}

func TestCopyAppend(t *testing.T) {
	type Ref = cadata.ID
	s := newStore(t)
	const N = 10000
	x := newTestRope(t, s, N)
	b := newTestBuilder(t, s)
	require.NoError(t, Copy(ctx, b, NewIterator[Ref](s, *x, TotalSpan())))
	v := "last value"
	require.NoError(t, b.Append(ctx, 0, []byte(v)))
	x, err := b.Finish(ctx)
	require.NoError(t, err)
	t.Log("depth", x.Depth)
	ents := collect(t, NewIterator[Ref](s, *x, TotalSpan()))
	last := ents[len(ents)-1]
	require.Equal(t, Path{N}, last.Path)
	require.Equal(t, v, string(last.Value))
}

type Ref = cadata.ID

func newTestBuilder(t *testing.T, s WriteStorage[Ref]) *Builder[Ref] {
	return NewBuilder(s, 1024, defaultMaxSize, nil)
}

func newTestRope(t *testing.T, s WriteStorage[Ref], n int) *Root[cadata.ID] {
	b := newTestBuilder(t, s)
	var value []byte
	for i := 0; i < n; i++ {
		value = fmt.Appendf(value[:0], "value %d", i)
		require.NoError(t, b.Append(ctx, 0, value))
	}
	root, err := b.Finish(ctx)
	require.NoError(t, err)
	return root
}

func newStore(t testing.TB) WriteStorage[Ref] {
	s := cadata.NewMem(gdat.Hash, 1<<20)
	return writeStore{
		storage: storage{s},
		s:       s,
	}
}

func collect[Ref any](t testing.TB, it *Iterator[Ref]) (ret []Entry) {
	for {
		var ent Entry
		err := it.Next(ctx, &ent)
		if errors.Is(err, EOS()) {
			break
		}
		require.NoError(t, err)
		ret = append(ret, ent)
	}
	return ret
}
