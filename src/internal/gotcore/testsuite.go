package gotcore

import (
	"context"
	"strconv"
	"testing"

	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestSpace(t *testing.T, newSpace func(t testing.TB) Space) {
	t.Run("CreateGet", func(t *testing.T) {
		ctx := testutil.Context(t)
		x := newSpace(t)
		b, err := doInspect(ctx, x, "test")
		require.ErrorIs(t, err, ErrNotExist)
		require.Nil(t, b)
		_, err = doCreate(ctx, x, "test", Metadata{})
		require.NoError(t, err)
		b, err = doInspect(ctx, x, "test")
		require.NoError(t, err)
		require.NotNil(t, b)
	})
	t.Run("List", func(t *testing.T) {
		ctx := testutil.Context(t)
		x := newSpace(t)
		const N = 20
		t.Log("creating", N, "markes")
		for i := 0; i < N; i++ {
			_, err := doCreate(ctx, x, "test"+strconv.Itoa(i), Metadata{})
			require.NoError(t, err)
		}
		t.Log("done creating markes, now listing...")
		names, err := doList(ctx, x, TotalSpan(), 0)
		require.NoError(t, err)
		require.Len(t, names, N)
	})
	t.Run("Delete", func(t *testing.T) {
		ctx := testutil.Context(t)
		x := newSpace(t)
		var err error
		_, err = doCreate(ctx, x, "test1", Metadata{})
		require.NoError(t, err)
		_, err = doCreate(ctx, x, "test2", Metadata{})
		require.NoError(t, err)

		_, err = doInspect(ctx, x, "test1")
		require.NoError(t, err)
		_, err = doInspect(ctx, x, "test2")
		require.NoError(t, err)

		err = doDelete(ctx, x, "test1")
		require.NoError(t, err)

		_, err = doInspect(ctx, x, "test1")
		require.ErrorIs(t, err, ErrNotExist)
		_, err = doInspect(ctx, x, "test2")
		require.NoError(t, err)
	})
}

func doInspect(ctx context.Context, sp Space, name string) (*Info, error) {
	var info *Info
	err := sp.Do(ctx, false, func(st SpaceTx) error {
		var err error
		info, err = st.Inspect(ctx, name)
		return err
	})
	return info, err
}

func doCreate(ctx context.Context, sp Space, name string, cfg Metadata) (*Info, error) {
	var info *Info
	err := sp.Do(ctx, true, func(st SpaceTx) error {
		var err error
		info, err = st.Create(ctx, name, cfg)
		return err
	})
	return info, err
}

func doList(ctx context.Context, sp Space, span Span, limit int) ([]string, error) {
	var names []string
	err := sp.Do(ctx, false, func(st SpaceTx) error {
		for name, err := range st.All(ctx) {
			if err != nil {
				return err
			}
			names = append(names, name)
		}
		return nil
	})
	return names, err
}

func doDelete(ctx context.Context, sp Space, name string) error {
	return sp.Do(ctx, true, func(st SpaceTx) error {
		return st.Delete(ctx, name)
	})
}
