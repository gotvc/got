package gottar

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/gotvc/got/pkg/gotfs"
)

// WriteTAR writes the gotfs instance at root to tw.
func WriteTAR(ctx context.Context, fsop *gotfs.Operator, ms, ds cadata.Store, root gotfs.Root, tw *tar.Writer) error {
	return fsop.ForEach(ctx, ms, root, "", func(p string, info *gotfs.Info) error {
		mode := posixfs.FileMode(info.Mode)
		var size int64
		if mode.IsRegular() {
			s, err := fsop.SizeOfFile(ctx, ms, root, "")
			if err != nil {
				return err
			}
			size = int64(s)
		}
		if err := tw.WriteHeader(&tar.Header{
			Typeflag: typeFlagFromMode(mode),
			Name:     p,
			Mode:     int64(mode),
			Xattrs:   info.Labels,
			Size:     size,
		}); err != nil {
			return err
		}
		if mode.IsRegular() {
			r, err := fsop.NewReader(ctx, ms, ds, root, p)
			if err != nil {
				return err
			}
			if _, err := io.Copy(tw, r); err != nil {
				return err
			}
		}
		return nil
	})
}

// ReadTAR copies the filesystem read from tr to b.
func ReadTAR(ctx context.Context, b *gotfs.Builder, tr *tar.Reader) error {
	for {
		th, err := tr.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		mode := th.FileInfo().Mode()
		switch {
		case mode.IsDir():
			if err := b.Mkdir(th.Name, mode); err != nil {
				return err
			}
		case mode.IsRegular():
			if err := b.BeginFile(th.Name, mode); err != nil {
				return err
			}
			if _, err := io.Copy(b, tr); err != nil {
				return err
			}
		default:
			return fmt.Errorf("gottar: unrecognized mode %v", th.Mode)
		}
	}
	return nil
}

func typeFlagFromMode(mode posixfs.FileMode) byte {
	switch {
	case mode.IsDir():
		return tar.TypeDir
	case mode.IsRegular():
		return tar.TypeReg
	default:
		return 0
	}
}
