package gotrepo

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"errors"

	"github.com/brendoncarroll/go-state/cadata"
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

	db *bolt.DB
}

func newStoreManager(store Store, db *bolt.DB) *storeManager {
	return &storeManager{
		store: store,
		db:    db,
	}
}

func (sm *storeManager) Create(ctx context.Context) (ret StoreID, _ error) {
	err := sm.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(setsBucketName))
		if err != nil {
			return err
		}
		ret = 0
		for ret == 0 {
			var err error
			ret, err = b.NextSequence()
			if err != nil {
				return err
			}
		}
		return nil
	})
	return ret, err
}

func (sm *storeManager) Drop(ctx context.Context, sid StoreID) error {
	s := sm.Open(sid)
	return cadata.ForEach(ctx, s, cadata.Span{}, func(id cadata.ID) error {
		return s.Delete(ctx, id)
	})
}

func (sm *storeManager) Open(id StoreID) Store {
	return virtualStore{
		sm: sm,
		id: id,
	}
}

// Flush ensures all the database writes have been synced.
func (sm *storeManager) Flush(ctx context.Context) error {
	return sm.db.Sync()
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
		c, err := sm.getCount(tx, id)
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

func (sm *storeManager) post(ctx context.Context, sid StoreID, data []byte) (cadata.ID, error) {
	id := sm.store.Hash(data)
	sm.lock(id, false)
	defer sm.unlock(id, false)
	if err := sm.db.Update(func(tx *bolt.Tx) error {
		if exists, err := sm.isInSet(tx, sid, id); err != nil {
			return err
		} else if exists {
			return nil
		}
		if err := sm.addToSet(tx, sid, id); err != nil {
			return err
		}
		return sm.incrCount(tx, id)
	}); err != nil {
		return cadata.ID{}, err
	}
	return sm.maybePost(ctx, id, data)
}

func (sm *storeManager) add(ctx context.Context, sid StoreID, id cadata.ID) error {
	sm.lock(id, false)
	defer sm.unlock(id, false)
	return sm.db.Update(func(tx *bolt.Tx) error {
		// check that something else has referenced it.
		count, err := sm.getCount(tx, id)
		if err != nil {
			return err
		}
		if count < 1 {
			return cadata.ErrNotFound
		}
		// and that it's in the store
		if exists, err := cadata.Exists(ctx, sm.store, id); err != nil {
			return err
		} else if !exists {
			return cadata.ErrNotFound
		}
		if exists, err := sm.isInSet(tx, sid, id); err != nil {
			return err
		} else if exists {
			return nil
		}
		if err := sm.addToSet(tx, sid, id); err != nil {
			return err
		}
		return sm.incrCount(tx, id)
	})
}

func (sm *storeManager) get(ctx context.Context, sid StoreID, id cadata.ID, buf []byte) (int, error) {
	sm.lock(id, false)
	defer sm.unlock(id, false)
	exists, err := sm.exists(ctx, sid, id)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, cadata.ErrNotFound
	}
	return sm.store.Get(ctx, id, buf)
}

func (sm *storeManager) exists(ctx context.Context, sid StoreID, id cadata.ID) (bool, error) {
	var exists bool
	if err := sm.db.View(func(tx *bolt.Tx) error {
		var err error
		exists, err = sm.isInSet(tx, sid, id)
		return err
	}); err != nil {
		return false, err
	}
	return exists, nil
}

func (sm *storeManager) delete(ctx context.Context, sid StoreID, id cadata.ID) error {
	sm.lock(id, true)
	defer sm.unlock(id, true)
	if err := sm.db.Batch(func(tx *bolt.Tx) error {
		if exists, err := sm.isInSet(tx, sid, id); err != nil {
			return err
		} else if !exists {
			return nil
		}
		if err := sm.removeFromSet(tx, sid, id); err != nil {
			return err
		}
		return sm.decrCount(tx, id)
	}); err != nil {
		return err
	}
	return sm.maybeDelete(ctx, id)
}

func (sm *storeManager) list(ctx context.Context, sid StoreID, span cadata.Span, ids []cadata.ID) (int, error) {
	var n int
	stopIter := errors.New("stop")
	err := sm.db.View(func(tx *bolt.Tx) error {
		n = 0
		return sm.forEachInSet(tx, sid, span, func(id cadata.ID) error {
			if n >= len(ids) {
				return stopIter
			}
			ids[n] = id
			n++
			return nil
		})
	})
	if err == stopIter {
		err = nil
	}
	return n, err
}

func (sm *storeManager) copyAll(ctx context.Context, src, dst StoreID) error {
	return sm.db.Batch(func(tx *bolt.Tx) error {
		return sm.forEachInSet(tx, src, cadata.Span{}, func(id cadata.ID) error {
			if exists, err := sm.isInSet(tx, dst, id); err != nil {
				return err
			} else if exists {
				return nil
			}
			if err := sm.addToSet(tx, dst, id); err != nil {
				return err
			}
			return sm.incrCount(tx, id)
		})
	})
}

func (sm *storeManager) addToSet(tx *bolt.Tx, setID StoreID, id cadata.ID) error {
	b, err := tx.CreateBucketIfNotExists([]byte(setsBucketName))
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

func (sm *storeManager) removeFromSet(tx *bolt.Tx, setID StoreID, id cadata.ID) error {
	b, err := tx.CreateBucketIfNotExists([]byte(setsBucketName))
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

func (sm *storeManager) isInSet(tx *bolt.Tx, setID StoreID, id cadata.ID) (bool, error) {
	b := tx.Bucket([]byte(setsBucketName))
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

func (sm *storeManager) forEachInSet(tx *bolt.Tx, setID StoreID, span cadata.Span, fn func(cadata.ID) error) error {
	b := tx.Bucket([]byte(setsBucketName))
	var setIDBuf [8]byte
	binary.BigEndian.PutUint64(setIDBuf[:], setID)
	setBucket := b.Bucket(setIDBuf[:])
	if setBucket == nil {
		return nil
	}
	begin := cadata.BeginFromSpan(span)
	c := setBucket.Cursor()
	for k, _ := c.Seek(begin[:]); k != nil; k, _ = c.Next() {
		id := cadata.IDFromBytes(k)
		if !span.Contains(id, idCompare) {
			break
		}
		if err := fn(id); err != nil {
			return err
		}
	}
	return nil
}

func (sm *storeManager) decrCount(tx *bolt.Tx, id cadata.ID) error {
	b, err := tx.CreateBucketIfNotExists([]byte(refCountBucketName))
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

func (sm *storeManager) incrCount(tx *bolt.Tx, id cadata.ID) error {
	b, err := tx.CreateBucketIfNotExists([]byte(refCountBucketName))
	if err != nil {
		return err
	}
	x, err := getUvarint(b, id[:])
	if err != nil {
		return err
	}
	return putUvarint(b, id[:], x+1)
}

func (sm *storeManager) getCount(tx *bolt.Tx, id cadata.ID) (int, error) {
	b := tx.Bucket([]byte(refCountBucketName))
	if b == nil {
		return 0, nil
	}
	x, err := getUvarint(b, id[:])
	if err != nil {
		return 0, err
	}
	return x, nil
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
	return s.sm.post(ctx, s.id, data)
}

// Add implements cadata.Adder
func (s virtualStore) Add(ctx context.Context, id cadata.ID) error {
	return s.sm.add(ctx, s.id, id)
}

// Read implements cadata.Reader
func (s virtualStore) Get(ctx context.Context, id cadata.ID, buf []byte) (int, error) {
	return s.sm.get(ctx, s.id, id, buf)
}

// Exists implements cadata.Set
func (s virtualStore) Exists(ctx context.Context, id cadata.ID) (bool, error) {
	return s.sm.exists(ctx, s.id, id)
}

// Deleta implements cadata.Store
func (s virtualStore) Delete(ctx context.Context, id cadata.ID) error {
	return s.sm.delete(ctx, s.id, id)
}

// List implements cadata.Set
func (s virtualStore) List(ctx context.Context, span cadata.Span, ids []cadata.ID) (int, error) {
	return s.sm.list(ctx, s.id, span, ids)
}

func (s virtualStore) Hash(x []byte) cadata.ID {
	return s.sm.store.Hash(x)
}

func (s virtualStore) MaxSize() int {
	return s.sm.store.MaxSize()
}

func (s virtualStore) CopyAllFrom(ctx context.Context, src cadata.Store) error {
	vs2, ok := src.(virtualStore)
	if !ok {
		return cadata.CopyAllBasic(ctx, s, src)
	}
	return s.sm.copyAll(ctx, vs2.id, s.id)
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
		return 0, fmt.Errorf("could not read varint")
	}
	return int(x), nil
}

func idCompare(a, b cadata.ID) int {
	return bytes.Compare(a[:], b[:])
}
