package gottar

import (
	"archive/tar"
	"fmt"
	"io"
	"io/fs"
	"testing"

	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/stores"
	"github.com/gotvc/got/pkg/testutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestRead(t *testing.T) {
	ctx := testutil.Context(t)
	ms, ds := stores.NewMem(), stores.NewMem()
	fsop := gotfs.NewOperator()
	err := WithPipe(func(w io.Writer) error {
		tw := tar.NewWriter(w)
		if err := tw.WriteHeader(&tar.Header{
			Name: "/",
			Mode: int64(fs.ModeDir | 0o755),
		}); err != nil {
			return err
		}
		for i := 0; i < 10; i++ {
			testData := "test data\n"
			if err := tw.WriteHeader(&tar.Header{
				Name: fmt.Sprintf("/text-%04d.txt", i),
				Mode: int64(0o644),
				Size: int64(len(testData)),
			}); err != nil {
				return err
			}
			if _, err := tw.Write([]byte(testData)); err != nil {
				return err
			}
		}
		return tw.Close()
	}, func(r io.Reader) error {
		b := fsop.NewBuilder(ctx, ms, ds)
		tr := tar.NewReader(r)
		if err := ReadTAR(ctx, b, tr); err != nil {
			return err
		}
		_, err := b.Finish()
		return err
	})
	require.NoError(t, err)
}

func WithPipe(wfn func(w io.Writer) error, rfn func(r io.Reader) error) error {
	pr, pw := io.Pipe()
	eg := errgroup.Group{}
	eg.Go(func() (retErr error) {
		defer func() { pw.CloseWithError(retErr) }()
		return wfn(pw)
	})
	eg.Go(func() (retErr error) {
		defer func() { pr.CloseWithError(retErr) }()
		return rfn(pr)
	})
	return eg.Wait()
}
