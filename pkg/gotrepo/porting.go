package gotrepo

import (
	"context"
	"encoding/binary"
	"encoding/json"

	"github.com/brendoncarroll/go-state"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/porting"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

// GetImportStores returns a store triple for importing the branch.
func (r *Repo) GetImportStores(ctx context.Context, branchName string) (*branches.StoreTriple, error) {
	b, err := r.GetBranch(ctx, branchName)
	if err != nil {
		return nil, err
	}
	return r.getImportTriple(ctx, b)
}

func (r *Repo) getImporter(ctx context.Context, b *branches.Branch) (*porting.Importer, error) {
	salt := saltFromBytes(b.Salt)
	saltHash := gdat.Hash(salt[:])
	st, err := r.getImportTriple(ctx, b)
	if err != nil {
		return nil, err
	}
	fsop := r.getFSOp(b)
	cache := newPortingCache(r.db, saltHash)
	return porting.NewImporter(fsop, cache, st.FS, st.Raw), nil
}

func (r *Repo) getExporter(b *branches.Branch) *porting.Exporter {
	fsop := r.getFSOp(b)
	salt := saltFromBytes(b.Salt)
	saltHash := gdat.Hash(salt[:])
	cache := newPortingCache(r.db, saltHash)
	return porting.NewExporter(fsop, cache, r.workingDir)
}

func (r *Repo) getImportTriple(ctx context.Context, b *branches.Branch) (ret *branches.StoreTriple, _ error) {
	salt := saltFromBytes(b.Salt)
	saltHash := gdat.Hash(salt[:])
	ids := [3]uint64{}
	err := r.db.Update(func(tx *bolt.Tx) error {
		ids = [3]uint64{}
		b, err := tx.CreateBucketIfNotExists([]byte(bucketImportStores))
		if err != nil {
			return err
		}
		v := b.Get(saltHash[:])
		if v == nil {
			v = make([]byte, 8*3)
			for i := 0; i < 3; i++ {
				// TODO: maybe don't do this in a transaction.
				// It is a different database, so it won't deadlock.
				id, err := r.storeManager.Create(ctx)
				if err != nil {
					return err
				}
				binary.BigEndian.PutUint64(v[8*i:], id)
			}
			if err := b.Put(saltHash[:], v); err != nil {
				return err
			}
		}
		if len(v) != 8*3 {
			return errors.New("bad length for staging store triple")
		}
		for i := 0; i < 3; i++ {
			ids[i] = binary.BigEndian.Uint64(v[8*i:])
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &branches.StoreTriple{
		Raw: r.storeManager.Open(ids[0]),
		FS:  r.storeManager.Open(ids[1]),
		VC:  r.storeManager.Open(ids[2]),
	}, nil
}

type portingCache struct {
	saltHash [32]byte
}

func newPortingCache(db *bolt.DB, saltHash [32]byte) *portingCache {
	return &portingCache{
		saltHash: saltHash,
	}
}

func (c *portingCache) Get(ctx context.Context, p string) (porting.Entry, error) {
	return porting.Entry{}, state.ErrNotFound
}

func (c *portingCache) Put(ctx context.Context, p string, ent porting.Entry) error {
	_, err := json.Marshal(ent)
	if err != nil {
		return err
	}
	return nil
}

func (c *portingCache) Delete(ctx context.Context, p string) error {
	return nil
}

func (c *portingCache) List(ctx context.Context, span state.Span[string], ks []string) (int, error) {
	return 0, nil
}

func saltFromBytes(x []byte) *[32]byte {
	var salt [32]byte
	copy(salt[:], x)
	return &salt
}
