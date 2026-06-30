package gotfs

import (
	"testing"

	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestDeltaWriterPutInfo(t *testing.T) {
	ctx := testutil.Context(t)
	mach := NewMachine(Params{})
	s := stores.NewMem()

	root, err := mach.NewEmpty(ctx, s, 0o755)
	require.NoError(t, err)

	dw := mach.NewDeltaWriter(s)
	require.NoError(t, dw.PutInfo(ctx, "a", Info{Mode: 0o644}))
	require.NoError(t, dw.PutInfo(ctx, "b", Info{Mode: 0o755}))
	delta, err := dw.Finish(ctx)
	require.NoError(t, err)

	applied, err := mach.gotkv.Apply(ctx, s, root.ToGotKV(), gotkv.Delta(delta))
	require.NoError(t, err)
	appliedRoot := Root{Ref: applied.Ref, Depth: applied.Depth}

	info, err := mach.GetInfo(ctx, s, appliedRoot, "a")
	require.NoError(t, err)
	require.Equal(t, 0o644, int(info.Mode))
	info, err = mach.GetInfo(ctx, s, appliedRoot, "b")
	require.NoError(t, err)
	require.Equal(t, 0o755, int(info.Mode))
}

func TestDeltaWriterPutAllData(t *testing.T) {
	ctx := testutil.Context(t)
	mach := NewMachine(Params{})
	s := stores.NewMem()

	root, err := mach.NewEmpty(ctx, s, 0o755)
	require.NoError(t, err)
	root, err = mach.PutInfo(ctx, s, *root, "f", &Info{Mode: 0o644})
	require.NoError(t, err)

	dw := mach.NewDeltaWriter(s)
	exts := []Extent{
		{Length: 100},
		{Length: 200},
	}
	require.NoError(t, dw.PutAllFileData(ctx, "f", exts))
	delta, err := dw.Finish(ctx)
	require.NoError(t, err)

	applied, err := mach.gotkv.Apply(ctx, s, root.ToGotKV(), gotkv.Delta(delta))
	require.NoError(t, err)
	appliedRoot := Root{Ref: applied.Ref, Depth: applied.Depth}

	size, err := mach.SizeOfFile(ctx, s, appliedRoot, "f")
	require.NoError(t, err)
	require.Equal(t, uint64(300), size)
}

func TestDeltaWriterDeletePath(t *testing.T) {
	ctx := testutil.Context(t)
	mach := NewMachine(Params{})
	s := stores.NewMem()

	root, err := mach.NewEmpty(ctx, s, 0o755)
	require.NoError(t, err)
	root, err = mach.PutInfo(ctx, s, *root, "f", &Info{Mode: 0o644})
	require.NoError(t, err)

	exists, err := mach.Exists(ctx, s, *root, "f")
	require.NoError(t, err)
	require.True(t, exists)

	dw := mach.NewDeltaWriter(s)
	require.NoError(t, dw.DeletePath(ctx, "f"))
	delta, err := dw.Finish(ctx)
	require.NoError(t, err)

	applied, err := mach.gotkv.Apply(ctx, s, root.ToGotKV(), gotkv.Delta(delta))
	require.NoError(t, err)
	appliedRoot := Root{Ref: applied.Ref, Depth: applied.Depth}

	exists, err = mach.Exists(ctx, s, appliedRoot, "f")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestDeltaWriterReadFromDiffer(t *testing.T) {
	ctx := testutil.Context(t)
	mach := NewMachine(Params{})
	s := stores.NewMem()

	empty, err := mach.NewEmpty(ctx, s, 0o755)
	require.NoError(t, err)

	target, err := mach.NewEmpty(ctx, s, 0o755)
	require.NoError(t, err)
	target, err = mach.PutInfo(ctx, s, *target, "a", &Info{Mode: 0o644})
	require.NoError(t, err)
	target, err = mach.PutInfo(ctx, s, *target, "b", &Info{Mode: 0o755})
	require.NoError(t, err)

	differ := mach.NewDiffer(s, *empty, *target)
	dw := mach.NewDeltaWriter(s)
	require.NoError(t, dw.ReadFromDiffer(ctx, differ))
	delta, err := dw.Finish(ctx)
	require.NoError(t, err)

	applied, err := mach.gotkv.Apply(ctx, s, empty.ToGotKV(), gotkv.Delta(delta))
	require.NoError(t, err)
	appliedRoot := Root{Ref: applied.Ref, Depth: applied.Depth}

	info, err := mach.GetInfo(ctx, s, appliedRoot, "a")
	require.NoError(t, err)
	require.Equal(t, 0o644, int(info.Mode))
	info, err = mach.GetInfo(ctx, s, appliedRoot, "b")
	require.NoError(t, err)
	require.Equal(t, 0o755, int(info.Mode))
}

func TestDeltaWriterDeleteDiffer(t *testing.T) {
	ctx := testutil.Context(t)
	mach := NewMachine(Params{})
	s := stores.NewMem()

	source, err := mach.NewEmpty(ctx, s, 0o755)
	require.NoError(t, err)
	source, err = mach.PutInfo(ctx, s, *source, "a", &Info{Mode: 0o644})
	require.NoError(t, err)

	empty, err := mach.NewEmpty(ctx, s, 0o755)
	require.NoError(t, err)

	differ := mach.NewDiffer(s, *source, *empty)
	dw := mach.NewDeltaWriter(s)
	require.NoError(t, dw.ReadFromDiffer(ctx, differ))
	delta, err := dw.Finish(ctx)
	require.NoError(t, err)

	applied, err := mach.gotkv.Apply(ctx, s, source.ToGotKV(), gotkv.Delta(delta))
	require.NoError(t, err)
	appliedRoot := Root{Ref: applied.Ref, Depth: applied.Depth}

	exists, err := mach.Exists(ctx, s, appliedRoot, "a")
	require.NoError(t, err)
	require.False(t, exists)
}
