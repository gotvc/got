package gotcore

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"testing"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotvc"
	"github.com/gotvc/got/src/internal/stores"
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
		t.Log("creating", N, "marks")
		for i := 0; i < N; i++ {
			_, err := doCreate(ctx, x, "test"+strconv.Itoa(i), Metadata{})
			require.NoError(t, err)
		}
		t.Log("done creating marks, now listing...")
		names, err := doList(ctx, x)
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
	t.Run("Sync", func(t *testing.T) {
		TestSync(t, newSpace)
	})
}

func TestSync(t *testing.T, setup func(testing.TB) Space) {
	ctx := testutil.Context(t)
	type Step struct {
		Snaps map[string]*Snap
		Force bool
		Err   error
	}
	type testCase struct {
		// Store is used to store any seed data needed for the test.
		Store stores.RW
		// Steps contains the set of Mark names, and the snapshots they point to.
		Steps []Step
	}

	cfg := DSConfig{}
	s := stores.NewMem()
	snap0 := makeSnap(t, cfg, s, nil, makeFS(t, s, map[string]string{
		"a": "0",
	}))
	tcs := []testCase{
		{

			Store: s,
			Steps: []Step{
				{
					Snaps: map[string]*Snap{
						"a": snap0,
					},
					Force: false,
				},
			},
		},
		{
			Store: s,
			Steps: []Step{
				{
					Snaps: map[string]*Snap{
						"a": snap0,
					},
					Force: false,
				},
				{
					Snaps: map[string]*Snap{
						"a": makeSnap(t, cfg, s, []Snap{*snap0}, makeFS(t, s, map[string]string{
							"a": "1",
						})),
					},
					Force: false,
				},
			},
		},
	}

	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			src := setup(t)
			dst := setup(t)
			for _, step := range tc.Steps {
				require.NoError(t, src.Do(ctx, true, func(st SpaceTx) error {
					for k, v := range step.Snaps {
						if _, err := st.Create(ctx, k, Metadata{}); err != nil && !errors.Is(err, ErrExists) {
							return err
						}
						mtx, err := NewMarkTx(ctx, st, k)
						if err != nil {
							return err
						}
						if err := mtx.Modify(ctx, func(mctx ModifyCtx) (*Snap, error) {
							if v != nil {
								srcStores := [3]stores.Reading{tc.Store, tc.Store, tc.Store}
								if err := mctx.Sync(ctx, srcStores, *v); err != nil {
									return nil, err
								}
							}
							return v, nil
						}); err != nil {
							return err
						}
					}
					return nil
				}))
				require.NoError(t, SyncSpaces(ctx, SyncSpacesTask{Src: src, Dst: dst}))
				require.NoError(t, dst.Do(ctx, false, func(st SpaceTx) error {
					for name, err := range st.All(ctx) {
						if err != nil {
							return err
						}
						if _, exists := step.Snaps[name]; !exists {
							t.Errorf("space has extra mark %s", name)
						}
					}
					for k := range step.Snaps {
						_, err := st.Inspect(ctx, k)
						require.NoError(t, err)
					}
					return nil
				}))
			}
		})
	}
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

func doList(ctx context.Context, sp Space) ([]string, error) {
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

func makeFS(t testing.TB, s stores.RW, files map[string]string) gotfs.Root {
	ctx := testutil.Context(t)
	fsmach := GotFS(DSConfig{})
	ps := slices.Collect(maps.Keys(files))
	b := fsmach.NewBuilder(ctx, s, s)
	require.NoError(t, b.Mkdir("", 0o755))
	for _, p := range ps {
		require.NoError(t, b.BeginFile(p, 0o644))
		_, err := b.Write([]byte(files[p]))
		require.NoError(t, err)
	}
	root, err := b.Finish()
	require.NoError(t, err)
	return *root
}

func makeSnap(t testing.TB, cfg DSConfig, s stores.Writing, parents []Snap, fsroot gotfs.Root) *Snap {
	ctx := testutil.Context(t)
	vcmach := gotvc.NewMachine(ParsePayload, gotvc.Config{Salt: cfg.Salt})
	snap, err := vcmach.NewSnapshot(ctx, s, gotvc.SnapshotParams[Payload]{
		Parents: parents,
		Payload: Payload{
			Root: fsroot,
		},
	})
	require.NoError(t, err)
	return snap
}
