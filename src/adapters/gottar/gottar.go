package gottar

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/gotvc/got/src/gotfs"
	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/state/posixfs"
)

// WriteTAR writes the gotfs instance at root to tw.
func WriteTAR(ctx context.Context, fsag *gotfs.Machine, ms, ds cadata.Store, root gotfs.Root, tw *tar.Writer) error {
	return fsag.ForEach(ctx, ms, root, "", func(p string, info *gotfs.Info) error {
		mode := posixfs.FileMode(info.Mode)
		var size int64
		if mode.IsRegular() {
			s, err := fsag.SizeOfFile(ctx, ms, root, "")
			if err != nil {
				return err
			}
			size = int64(s)
		}
		if err := tw.WriteHeader(&tar.Header{
			Typeflag: typeFlagFromMode(mode),
			Name:     p,
			Mode:     int64(mode),
			Xattrs:   convertAttrs(info.Attrs),
			Size:     size,
		}); err != nil {
			return err
		}
		if mode.IsRegular() {
			r, err := fsag.NewReader(ctx, ms, ds, root, p)
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

func convertAttrs(x map[string][]byte) map[string]string {
	y := make(map[string]string)
	for k, v := range x {
		y[k] = string(v)
	}
	return y
}
