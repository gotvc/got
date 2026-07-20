package gotfsdelta

import (
	"testing"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv/gotkvdelta"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestDeltaWriterPutInfo(t *testing.T) {
	ctx := testutil.Context(t)
	fsmach, mach, s := setup(t)

	root, err := fsmach.NewEmpty(ctx, s, 0o755)
	require.NoError(t, err)

	dw := mach.NewDeltaWriter(s)
	require.NoError(t, dw.PutInfo(ctx, "a", Info{Mode: 0o644}))
	require.NoError(t, dw.PutInfo(ctx, "b", Info{Mode: 0o755}))
	delta, err := dw.Finish(ctx)
	require.NoError(t, err)

	applied, err := mach.kvd.Apply(ctx, s, root.ToGotKV(), gotkvdelta.Delta(delta))
	require.NoError(t, err)
	appliedRoot := Root{Ref: applied.Ref, Depth: applied.Depth}

	info, err := fsmach.GetInfo(ctx, s, appliedRoot, "a")
	require.NoError(t, err)
	require.Equal(t, 0o644, int(info.Mode))
	info, err = fsmach.GetInfo(ctx, s, appliedRoot, "b")
	require.NoError(t, err)
	require.Equal(t, 0o755, int(info.Mode))
}

func TestDeltaWriterPutAllData(t *testing.T) {
	ctx := testutil.Context(t)
	fsmach, mach, s := setup(t)

	root, err := fsmach.NewEmpty(ctx, s, 0o755)
	require.NoError(t, err)
	root, err = fsmach.PutInfo(ctx, s, root, "f", &Info{Mode: 0o644})
	require.NoError(t, err)

	dw := mach.NewDeltaWriter(s)
	exts := []Extent{
		{Length: 100},
		{Length: 200},
	}
	require.NoError(t, dw.PutAllFileData(ctx, "f", exts))
	delta, err := dw.Finish(ctx)
	require.NoError(t, err)

	applied, err := mach.kvd.Apply(ctx, s, root.ToGotKV(), gotkvdelta.Delta(delta))
	require.NoError(t, err)
	appliedRoot := Root{Ref: applied.Ref, Depth: applied.Depth}

	size, err := fsmach.SizeOfFile(ctx, s, appliedRoot, "f")
	require.NoError(t, err)
	require.Equal(t, uint64(300), size)
}

func TestDeltaWriterDeletePath(t *testing.T) {
	ctx := testutil.Context(t)
	fsmach, mach, s := setup(t)

	root, err := fsmach.NewEmpty(ctx, s, 0o755)
	require.NoError(t, err)
	root, err = fsmach.PutInfo(ctx, s, root, "f", &Info{Mode: 0o644})
	require.NoError(t, err)

	exists, err := fsmach.Exists(ctx, s, root, "f")
	require.NoError(t, err)
	require.True(t, exists)

	dw := mach.NewDeltaWriter(s)
	require.NoError(t, dw.DeletePath(ctx, "f"))
	delta, err := dw.Finish(ctx)
	require.NoError(t, err)

	applied, err := mach.kvd.Apply(ctx, s, root.ToGotKV(), gotkvdelta.Delta(delta))
	require.NoError(t, err)
	appliedRoot := Root{Ref: applied.Ref, Depth: applied.Depth}

	exists, err = fsmach.Exists(ctx, s, appliedRoot, "f")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestDeltaWriterReadFromDiffer(t *testing.T) {
	ctx := testutil.Context(t)
	fsmach, mach, s := setup(t)

	empty, err := fsmach.NewEmpty(ctx, s, 0o755)
	require.NoError(t, err)

	target, err := fsmach.NewEmpty(ctx, s, 0o755)
	require.NoError(t, err)
	target, err = fsmach.PutInfo(ctx, s, target, "a", &Info{Mode: 0o644})
	require.NoError(t, err)
	target, err = fsmach.PutInfo(ctx, s, target, "b", &Info{Mode: 0o755})
	require.NoError(t, err)

	differ := fsmach.NewDiffer(s, empty, target)
	dw := mach.NewDeltaWriter(s)
	require.NoError(t, dw.ReadFromDiffer(ctx, differ))
	delta, err := dw.Finish(ctx)
	require.NoError(t, err)

	applied, err := mach.kvd.Apply(ctx, s, empty.ToGotKV(), gotkvdelta.Delta(delta))
	require.NoError(t, err)
	appliedRoot := Root{Ref: applied.Ref, Depth: applied.Depth}

	info, err := fsmach.GetInfo(ctx, s, appliedRoot, "a")
	require.NoError(t, err)
	require.Equal(t, 0o644, int(info.Mode))
	info, err = fsmach.GetInfo(ctx, s, appliedRoot, "b")
	require.NoError(t, err)
	require.Equal(t, 0o755, int(info.Mode))
}

func TestDeltaWriterDeleteDiffer(t *testing.T) {
	ctx := testutil.Context(t)
	fsmach := gotfs.NewMachine(gotfs.Params{})
	mach := NewMachine(&fsmach, [32]byte{})
	s := stores.NewMem()

	source, err := fsmach.NewEmpty(ctx, s, 0o755)
	require.NoError(t, err)
	source, err = fsmach.PutInfo(ctx, s, source, "a", &Info{Mode: 0o644})
	require.NoError(t, err)

	empty, err := fsmach.NewEmpty(ctx, s, 0o755)
	require.NoError(t, err)

	differ := fsmach.NewDiffer(s, source, empty)
	dw := mach.NewDeltaWriter(s)
	require.NoError(t, dw.ReadFromDiffer(ctx, differ))
	delta, err := dw.Finish(ctx)
	require.NoError(t, err)

	applied, err := mach.kvd.Apply(ctx, s, source.ToGotKV(), gotkvdelta.Delta(delta))
	require.NoError(t, err)
	appliedRoot := Root{Ref: applied.Ref, Depth: applied.Depth}

	exists, err := fsmach.Exists(ctx, s, appliedRoot, "a")
	require.NoError(t, err)
	require.False(t, exists)
}

func setup(t *testing.T) (*gotfs.Machine, Machine, stores.RWD) {
	fsmach := gotfs.NewMachine(gotfs.Params{})
	mach := NewMachine(&fsmach, [32]byte{})
	s := stores.NewMem()
	return &fsmach, mach, s
}
