package got

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"log"

	"github.com/brendoncarroll/go-state/cells"
	"github.com/pkg/errors"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/got/pkg/branches"
	"github.com/brendoncarroll/got/pkg/gdat"
	"github.com/brendoncarroll/got/pkg/gotfs"
	"github.com/brendoncarroll/got/pkg/gotvc"
)

// SyncVolumes moves the commit in src and all it's data from to dst
// if the commit in dst is not an ancestor of src then an error is returned.
// that behavior can be disabled with force=true.
func syncVolumes(ctx context.Context, dst, src Volume, force bool) error {
	return applySnapshot(ctx, dst.Cell, func(x *gotvc.Snapshot) (*gotvc.Snapshot, error) {
		goal, err := getSnapshot(ctx, src.Cell)
		if err != nil {
			return nil, err
		}
		if x == nil {
			return goal, err
		}
		goalRef, err := gotvc.PostSnapshot(ctx, cadata.Void{}, *goal)
		if err != nil {
			return nil, err
		}
		xRef, err := gotvc.PostSnapshot(ctx, cadata.Void{}, *x)
		if err != nil {
			return nil, err
		}
		hasAncestor, err := gotvc.HasAncestor(ctx, src.VCStore, *goalRef, *xRef)
		if err != nil {
			return nil, err
		}
		if !force && !hasAncestor {
			return nil, errors.Errorf("cannot CAS, dst ref is not parent of src ref")
		}
		if err := syncStores(ctx, tripleFromVolume(dst), tripleFromVolume(src), *goal); err != nil {
			return nil, err
		}
		return goal, nil
	})
}

func (r *Repo) makeDefaultVolume() VolumeSpec {
	newRandom := func() *uint64 {
		x := randomUint64()
		return &x
	}
	cellSpec := CellSpec{
		Local: (*LocalCellSpec)(newRandom()),
	}
	cellSpec = CellSpec{
		Encrypted: &EncryptedCellSpec{
			Inner:  cellSpec,
			Secret: generateSecret(32),
		},
	}
	return VolumeSpec{
		Cell:     cellSpec,
		VCStore:  StoreSpec{Local: (*LocalStoreSpec)(newRandom())},
		FSStore:  StoreSpec{Local: (*LocalStoreSpec)(newRandom())},
		RawStore: StoreSpec{Local: (*LocalStoreSpec)(newRandom())},
	}
}

func getSnapshot(ctx context.Context, c cells.Cell) (*Commit, error) {
	data, err := cells.GetBytes(ctx, c)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	var x Commit
	if err := json.Unmarshal(data, &x); err != nil {
		return nil, err
	}
	return &x, nil
}

func applySnapshot(ctx context.Context, c cells.Cell, fn func(*Commit) (*Commit, error)) error {
	return cells.Apply(ctx, c, func(data []byte) ([]byte, error) {
		var x *Commit
		if len(data) > 0 {
			x = &Commit{}
			if err := json.Unmarshal(data, &x); err != nil {
				return nil, err
			}
		}
		y, err := fn(x)
		if err != nil {
			return nil, err
		}
		if y == nil {
			return nil, nil
		}
		return json.Marshal(*y)
	})
}

type triple struct {
	VC, FS, Raw Store
}

func tripleFromVolume(vol branches.Volume) triple {
	return triple{
		VC:  vol.VCStore,
		FS:  vol.FSStore,
		Raw: vol.RawStore,
	}
}

func syncStores(ctx context.Context, dst, src triple, snap gotvc.Snapshot) error {
	log.Println("begin syncing stores")
	defer log.Println("done syncing stores")
	return gotvc.Sync(ctx, dst.VC, src.VC, snap, func(root gotfs.Root) error {
		return gotfs.Sync(ctx, dst.FS, src.FS, root, func(ref gdat.Ref) error {
			return cadata.Copy(ctx, dst.Raw, src.Raw, ref.CID)
		})
	})
}

func generateSecret(n int) []byte {
	x := make([]byte, n)
	if _, err := rand.Read(x); err != nil {
		panic(err)
	}
	return x
}

func randomUint64() uint64 {
	buf := [8]byte{}
	if _, err := rand.Read(buf[:]); err != nil {
		panic(err)
	}
	return binary.BigEndian.Uint64(buf[:])
}
