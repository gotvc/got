package gotfsfix

import (
	"context"
	"os"
	"path"
	"strings"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/stdctx/logctx"
)

// FixDirs rebuilds a filesystem, inserting any missing parent directories.
func FixDirs(ctx context.Context, fsmach *gotfs.Machine, ms stores.RW, root gotfs.Root) (*gotfs.Root, error) {
	it := fsmach.NewIterator(ms, root, gotkv.TotalSpan())
	kvmach := fsmach.MetadataKV()
	b := kvmach.NewBuilder(ms)

	var dirstack []string
	var ent gotfs.Entry
	for {
		if err := streams.NextUnit(ctx, &it, &ent); err != nil {
			if streams.IsEOS(err) {
				break
			}
			return nil, err
		}
		if ent.Key.IsInfo() {
			p := ent.Path()
			if err := ensureParents(ctx, &dirstack, b, p); err != nil {
				return nil, err
			}
			if ent.Info.Mode.IsDir() && p != "" {
				dirstack = append(dirstack, path.Base(p))
			}
		}
		if err := writeEntry(ctx, b, ent); err != nil {
			return nil, err
		}
	}
	return finish(ctx, b)
}

func writeEntry(ctx context.Context, b *gotkv.Builder, ent gotfs.Entry) error {
	var value []byte
	if ent.Key.IsInfo() {
		value = ent.Info.Marshal(value)
	} else {
		value, _ = ent.Value.Extent.MarshalBinary()
	}
	return b.Put(ctx, ent.Key.Marshal(nil), value)
}

func finish(ctx context.Context, b *gotkv.Builder) (*gotfs.Root, error) {
	kvr, err := b.Finish(ctx)
	if err != nil {
		return nil, err
	}
	return gotfs.Promote(ctx, gotfs.Segment{Contents: kvr, Span: gotkv.TotalSpan()})
}

// ensureParents makes sure all ancestor directories of p exist in the builder,
// including the root directory "".
// It uses dirstack to track which directories have already been written.
func ensureParents(ctx context.Context, dirstack *[]string, b *gotkv.Builder, p string) error {
	if p == "" {
		return nil
	}
	parts := strings.Split(p, "/")
	// Check how many leading parts of the path already match the dirstack.
	matched := 0
	for matched < len(parts)-1 && matched < len(*dirstack) && (*dirstack)[matched] == parts[matched] {
		matched++
	}
	// Write any missing ancestor directories.
	for i := matched; i < len(parts)-1; i++ {
		ancestor := strings.Join(parts[:i+1], "/")
		logctx.Infof(ctx, "inserting parent directory %s before %s", ancestor, p)
		if err := putDirInfo(ctx, b, ancestor); err != nil {
			return err
		}
	}
	// Update the dirstack if p itself is going to be a directory,
	// but we don't know that here — the caller writes the entry after us.
	// Update dirstack to reflect the ancestors we've ensured.
	*dirstack = append((*dirstack)[:0], parts[:len(parts)-1]...)
	return nil
}

func putDirInfo(ctx context.Context, b *gotkv.Builder, p string) error {
	info := gotfs.Info{Mode: 0o755 | os.ModeDir}
	key, err := gotfs.NewInfoKey(p)
	if err != nil {
		return err
	}
	return b.Put(ctx, key.Marshal(nil), info.Marshal(nil))
}
