package gotrepo

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"sync"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

const (
	setsBucketName     = "sets"
	refCountBucketName = "rcs"
)

type StoreID = uint64

type storeManager struct {
	store Store
	locks [256]sync.RWMutex

	db         *bolt.DB
	bucketName string
}

func newStoreManager(store Store, db *bolt.DB, bucketName string) *storeManager {
	return &storeManager{
		store:      store,
		db:         db,
		bucketName: bucketName,
	}
}

func (sm *storeManager) CreateStore() StoreID {
	buf := [8]byte{}
	if _, err := rand.Read(buf[:]); err != nil {
		panic(err)
	}
	id := binary.BigEndian.Uint64(buf[:])
	// TODO: check if the id already exists and re-roll
	return id
}

func (sm *storeManager) ExistsStore(id StoreID) (bool, error) {
	var exists bool
	if err := sm.db.View(func(tx *bolt.Tx) error {
		b, err := sm.bucket(tx)
		if err != nil {
			return err
		}
		yes, err := isNonEmpty(b, id)
		if err != nil {
			return err
		}
		exists = yes
		return nil
	}); err != nil {
		return false, err
	}
	return exists, nil
}

func (sm *storeManager) GetStore(id StoreID) Store {
	return virtualStore{
		sm: sm,
		id: id,
	}
}

func (sm *storeManager) maybePost(ctx context.Context, id cadata.ID, data []byte) (cadata.ID, error) {
	exists, err := cadata.Exists(ctx, sm.store, id)
	if err != nil {
		return cadata.ID{}, err
	}
	if exists {
		return id, nil
	}
	return sm.store.Post(ctx, data)
}

func (sm *storeManager) maybeDelete(ctx context.Context, id cadata.ID) error {
	if id == (cadata.ID{}) {
		panic("empty id")
	}
	var count int
	if err := sm.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(sm.bucketName))
		if b == nil {
			return nil
		}
		c, err := getCount(b, id)
		if err != nil {
			return err
		}
		count = c
		return nil
	}); err != nil {
		return err
	}
	if count > 0 {
		return nil
	}
	return sm.store.Delete(ctx, id)
}

func (sm *storeManager) bucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	if tx.Writable() {
		return tx.CreateBucketIfNotExists([]byte(sm.bucketName))
	} else {
		return tx.Bucket([]byte(sm.bucketName)), nil
	}
}

func (sm *storeManager) lock(id cadata.ID, deleteMode bool) {
	l := &sm.locks[id[0]]
	if deleteMode {
		l.Lock()
	} else {
		l.RLock()
	}
}

func (sm *storeManager) unlock(id cadata.ID, deleteMode bool) {
	l := &sm.locks[id[0]]
	if deleteMode {
		l.Unlock()
	} else {
		l.RUnlock()
	}
}

var _ interface {
	cadata.Store
	cadata.Adder
} = &virtualStore{}

type virtualStore struct {
	sm *storeManager
	id StoreID
}

// Post implements cadata.Poster
func (s virtualStore) Post(ctx context.Context, data []byte) (cadata.ID, error) {
	id := cadata.DefaultHash(data)
	s.sm.lock(id, false)
	defer s.sm.unlock(id, false)
	if err := s.sm.db.Batch(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(s.sm.bucketName))
		if err != nil {
			return err
		}
		if exists, err := isInSet(b, s.id, id); err != nil {
			return err
		} else if exists {
			return nil
		}
		if err := addToSet(b, s.id, id); err != nil {
			return err
		}
		return incrCount(b, id)
	}); err != nil {
		return cadata.ID{}, err
	}
	return s.sm.maybePost(ctx, id, data)
}

// Add implements cadata.Adder
func (s virtualStore) Add(ctx context.Context, id cadata.ID) error {
	s.sm.lock(id, false)
	defer s.sm.unlock(id, false)
	return s.sm.db.Batch(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(s.sm.bucketName))
		if err != nil {
			return err
		}
		// check that something else has referenced it.
		count, err := getCount(b, id)
		if err != nil {
			return err
		}
		if count < 1 {
			return cadata.ErrNotFound
		}
		// and that it's in the store
		if exists, err := cadata.Exists(ctx, s.sm.store, id); err != nil {
			return err
		} else if !exists {
			return cadata.ErrNotFound
		}
		if exists, err := isInSet(b, s.id, id); err != nil {
			return err
		} else if exists {
			return nil
		}
		if err := addToSet(b, s.id, id); err != nil {
			return err
		}
		return incrCount(b, id)
	})
}

// Read implements cadata.Reader
func (s virtualStore) Get(ctx context.Context, id cadata.ID, buf []byte) (int, error) {
	exists, err := cadata.Exists(ctx, s, id)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, cadata.ErrNotFound
	}
	return s.sm.store.Get(ctx, id, buf)
}

// Exists implements cadata.Set
func (s virtualStore) Exists(ctx context.Context, id cadata.ID) (bool, error) {
	var exists bool
	if err := s.sm.db.View(func(tx *bolt.Tx) error {
		b, err := s.sm.bucket(tx)
		if err != nil {
			return err
		}
		exists, err = isInSet(b, s.id, id)
		return err
	}); err != nil {
		return false, err
	}
	return exists, nil
}

// Deleta implements cadata.Store
func (s virtualStore) Delete(ctx context.Context, id cadata.ID) error {
	s.sm.lock(id, true)
	defer s.sm.unlock(id, true)
	if err := s.sm.db.Batch(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(setsBucketName))
		if err != nil {
			return err
		}
		if exists, err := isInSet(b, s.id, id); err != nil {
			return err
		} else if !exists {
			return nil
		}
		if err := removeFromSet(b, s.id, id); err != nil {
			return err
		}
		return decrCount(b, id)
	}); err != nil {
		return err
	}
	return s.sm.maybeDelete(ctx, id)
}

// List implements cadata.Set
func (s virtualStore) List(ctx context.Context, first []byte, ids []cadata.ID) (int, error) {
	var n int
	stopIter := errors.New("stop")
	err := s.sm.db.View(func(tx *bolt.Tx) error {
		b, _ := s.sm.bucket(tx)
		if b == nil {
			return nil
		}
		return forEachInSet(b, s.id, first, func(id cadata.ID) error {
			ids[n] = id
			n++
			if n == len(ids) {
				return stopIter
			}
			return nil
		})
	})
	if err != nil && err != stopIter {
		return 0, err
	}
	if err == stopIter {
		return 0, nil
	}
	return n, cadata.ErrEndOfList
}

func (s virtualStore) Hash(x []byte) cadata.ID {
	return cadata.DefaultHash(x)
}

func (s virtualStore) MaxSize() int {
	return s.sm.store.MaxSize()
}

func (s virtualStore) CopyAllFrom(ctx context.Context, src cadata.Store) error {
	vs2, ok := src.(virtualStore)
	if !ok {
		return cadata.CopyAllBasic(ctx, s, src)
	}
	return s.sm.db.Batch(func(tx *bolt.Tx) error {
		b, _ := s.sm.bucket(tx)
		if b == nil {
			return nil
		}
		return forEachInSet(b, vs2.id, nil, func(id cadata.ID) error {
			if exists, err := isInSet(b, s.id, id); err != nil {
				return err
			} else if exists {
				return nil
			}
			if err := addToSet(b, s.id, id); err != nil {
				return err
			}
			return incrCount(b, id)
		})
	})
}

func addToSet(b *bolt.Bucket, setID StoreID, id cadata.ID) error {
	b, err := b.CreateBucketIfNotExists([]byte(setsBucketName))
	if err != nil {
		return err
	}
	var setIDBuf [8]byte
	binary.BigEndian.PutUint64(setIDBuf[:], setID)
	setBucket, err := b.CreateBucketIfNotExists(setIDBuf[:])
	if err != nil {
		return err
	}
	return setBucket.Put(id[:], nil)
}

func removeFromSet(b *bolt.Bucket, setID StoreID, id cadata.ID) error {
	b, err := b.CreateBucketIfNotExists([]byte(setsBucketName))
	if err != nil {
		return err
	}
	var setIDBuf [8]byte
	binary.BigEndian.PutUint64(setIDBuf[:], setID)
	setBucket, err := b.CreateBucketIfNotExists(setIDBuf[:])
	if err != nil {
		return err
	}
	return setBucket.Delete(id[:])
}

func isInSet(b *bolt.Bucket, setID StoreID, id cadata.ID) (bool, error) {
	if b == nil {
		return false, nil
	}
	b = b.Bucket([]byte(setsBucketName))
	if b == nil {
		return false, nil
	}
	var setIDBuf [8]byte
	binary.BigEndian.PutUint64(setIDBuf[:], setID)
	setBucket := b.Bucket(setIDBuf[:])
	if setBucket == nil {
		return false, nil
	}
	v := setBucket.Get(id[:])
	return v != nil, nil
}

func forEachInSet(b *bolt.Bucket, setID StoreID, first []byte, fn func(cadata.ID) error) error {
	b = b.Bucket([]byte(setsBucketName))
	var setIDBuf [8]byte
	binary.BigEndian.PutUint64(setIDBuf[:], setID)
	setBucket := b.Bucket(setIDBuf[:])
	if setBucket == nil {
		return nil
	}
	c := setBucket.Cursor()
	for k, _ := c.Seek(first); k != nil; k, _ = c.Next() {
		id := cadata.IDFromBytes(k)
		if err := fn(id); err != nil {
			return err
		}
	}
	return nil
}

func isNonEmpty(b *bolt.Bucket, setID StoreID) (bool, error) {
	b = b.Bucket([]byte(setsBucketName))
	var setIDBuf [8]byte
	binary.BigEndian.PutUint64(setIDBuf[:], setID)
	setBucket := b.Bucket(setIDBuf[:])
	if setBucket == nil {
		return false, nil
	}
	c := b.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		return true, nil
	}
	return false, nil
}

func incrCount(b *bolt.Bucket, id cadata.ID) error {
	b, err := b.CreateBucketIfNotExists([]byte(refCountBucketName))
	if err != nil {
		return err
	}
	x, err := getUvarint(b, id[:])
	if err != nil {
		return err
	}
	return putUvarint(b, id[:], x+1)
}

func decrCount(b *bolt.Bucket, id cadata.ID) error {
	b, err := b.CreateBucketIfNotExists([]byte(refCountBucketName))
	if err != nil {
		return err
	}
	x, err := getUvarint(b, id[:])
	if err != nil {
		return err
	}
	if x-1 == 0 {
		return b.Delete(id[:])
	}
	return putUvarint(b, id[:], x-1)
}

func getCount(b *bolt.Bucket, id cadata.ID) (int, error) {
	b = b.Bucket([]byte(refCountBucketName))
	if b == nil {
		return 0, nil
	}
	x, err := getUvarint(b, id[:])
	if err != nil {
		return 0, err
	}
	return x, nil
}

func putUvarint(b *bolt.Bucket, key []byte, x int) error {
	buf := [binary.MaxVarintLen64]byte{}
	n := binary.PutUvarint(buf[:], uint64(x))
	return b.Put(key, buf[:n])
}

func getUvarint(b *bolt.Bucket, key []byte) (int, error) {
	v := b.Get(key)
	if len(v) == 0 {
		return 0, nil
	}
	x, n := binary.Uvarint(v)
	if n <= 0 {
		return 0, errors.Errorf("could not read varint")
	}
	return int(x), nil
}
