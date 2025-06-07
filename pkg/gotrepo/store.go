package gotrepo

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"

	"github.com/dgraph-io/badger/v3"
	"go.brendoncarroll.net/state/cadata"
	"golang.org/x/sync/semaphore"

	"github.com/gotvc/got/pkg/gotrepo/repodb"
)

var (
	setIDSequence = repodb.IDFromString("setq")
	setsTable     = repodb.IDFromString("sets")
	refCountTable = repodb.IDFromString("rcs")
)

type StoreID = uint64

type storeManager struct {
	store Store
	locks [256]*semaphore.Weighted

	db *badger.DB
}

func newStoreManager(store Store, db *badger.DB) *storeManager {
	var locks [256]*semaphore.Weighted
	for i := range locks {
		locks[i] = semaphore.NewWeighted(math.MaxInt64)
	}
	return &storeManager{
		store: store,
		db:    db,
		locks: locks,
	}
}

func (sm *storeManager) Create(ctx context.Context) (StoreID, error) {
	seq, err := sm.db.GetSequence(setIDSequence.Bytes(), 1)
	if err != nil {
		return 0, err
	}
	setID, err := seq.Next()
	if err != nil {
		return 0, err
	}
	if setID == 0 {
		setID, err = seq.Next()
		if err != nil {
			return 0, err
		}
	}
	return setID, err
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
	exists, err := sm.store.Exists(ctx, id)
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
	var count uint64
	if err := sm.db.View(func(tx *badger.Txn) error {
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

func (sm *storeManager) lock(ctx context.Context, id cadata.ID, deleteMode bool) error {
	l := sm.locks[id[0]]
	if deleteMode {
		return l.Acquire(ctx, math.MaxInt64)
	} else {
		return l.Acquire(ctx, 1)
	}
}

func (sm *storeManager) unlock(id cadata.ID, deleteMode bool) {
	l := sm.locks[id[0]]
	if deleteMode {
		l.Release(math.MaxInt64)
	} else {
		l.Release(1)
	}
}

func (sm *storeManager) post(ctx context.Context, sid StoreID, data []byte) (cadata.ID, error) {
	id := sm.store.Hash(data)
	if err := sm.lock(ctx, id, false); err != nil {
		return cadata.ID{}, err
	}
	defer sm.unlock(id, false)
	if err := sm.db.Update(func(tx *badger.Txn) error {
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
	if err := sm.lock(ctx, id, false); err != nil {
		return err
	}
	defer sm.unlock(id, false)
	return sm.db.Update(func(tx *badger.Txn) error {
		// check that something else has referenced it.
		count, err := sm.getCount(tx, id)
		if err != nil {
			return err
		}
		if count < 1 {
			return cadata.ErrNotFound{Key: id}
		}
		// and that it's in the store
		if exists, err := sm.store.Exists(ctx, id); err != nil {
			return err
		} else if !exists {
			return cadata.ErrNotFound{Key: id}
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
	if err := sm.lock(ctx, id, false); err != nil {
		return 0, err
	}
	defer sm.unlock(id, false)
	exists, err := sm.exists(ctx, sid, id)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, cadata.ErrNotFound{Key: id}
	}
	return sm.store.Get(ctx, id, buf)
}

func (sm *storeManager) exists(ctx context.Context, sid StoreID, id cadata.ID) (bool, error) {
	var exists bool
	if err := sm.db.View(func(tx *badger.Txn) error {
		var err error
		exists, err = sm.isInSet(tx, sid, id)
		return err
	}); err != nil {
		return false, err
	}
	return exists, nil
}

func (sm *storeManager) delete(ctx context.Context, sid StoreID, id cadata.ID) error {
	if err := sm.lock(ctx, id, true); err != nil {
		return err
	}
	defer sm.unlock(id, true)
	if err := sm.db.Update(func(tx *badger.Txn) error {
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
	err := sm.db.View(func(tx *badger.Txn) error {
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
	return sm.db.Update(func(tx *badger.Txn) error {
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

func (sm *storeManager) appendSetItemKey(out []byte, setID StoreID, id cadata.ID) []byte {
	out = repodb.Key(out, setsTable, nil)
	out = binary.BigEndian.AppendUint64(out, setID)
	out = append(out, id[:]...)
	return out
}

func (sm *storeManager) addToSet(tx *badger.Txn, setID StoreID, id cadata.ID) error {
	key := sm.appendSetItemKey(nil, setID, id)
	return tx.Set(key, nil)
}

func (sm *storeManager) removeFromSet(tx *badger.Txn, setID StoreID, id cadata.ID) error {
	key := sm.appendSetItemKey(nil, setID, id)
	return tx.Delete(key)
}

func (sm *storeManager) isInSet(tx *badger.Txn, setID StoreID, id cadata.ID) (bool, error) {
	key := sm.appendSetItemKey(nil, setID, id)
	_, err := tx.Get(key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (sm *storeManager) forEachInSet(tx *badger.Txn, setID StoreID, span cadata.Span, fn func(cadata.ID) error) error {
	var prefix []byte
	prefix = setsTable.AppendTo(prefix)
	prefix = binary.BigEndian.AppendUint64(prefix, setID)

	var first []byte
	first = append(first, prefix...)
	if lb, ok := span.LowerBound(); ok {
		first = append(first, lb[:]...)
	}
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	iter := tx.NewIterator(opts)
	defer iter.Close()
	for iter.Seek(first); iter.Valid(); iter.Next() {
		item := iter.Item()
		k := item.Key()
		id := cadata.IDFromBytes(k[len(prefix):])
		if !span.Contains(id, idCompare) {
			// TODO: change this to break
			continue
		}
		if err := fn(id); err != nil {
			return err
		}
	}
	return nil
}

func (sm *storeManager) appendRefCountKey(out []byte, id cadata.ID) []byte {
	return repodb.Key(out, refCountTable, id[:])
}

func (sm *storeManager) decrCount(tx *badger.Txn, id cadata.ID) error {
	key := sm.appendRefCountKey(nil, id)
	x, err := getUvarint(tx, key)
	if err != nil {
		return err
	}
	if x-1 == 0 {
		return tx.Delete(key)
	} else {
		return putUvarint(tx, key, x-1)
	}
}

func (sm *storeManager) incrCount(tx *badger.Txn, id cadata.ID) error {
	key := sm.appendRefCountKey(nil, id)
	x, err := getUvarint(tx, key)
	if err != nil {
		return err
	}
	return putUvarint(tx, key, x+1)
}

func (sm *storeManager) getCount(tx *badger.Txn, id cadata.ID) (uint64, error) {
	key := sm.appendRefCountKey(nil, id)
	return getUvarint(tx, key)
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

func putUvarint(tx *badger.Txn, key []byte, x uint64) error {
	buf := [binary.MaxVarintLen64]byte{}
	n := binary.PutUvarint(buf[:], uint64(x))
	return tx.Set(key, buf[:n])
}

func getUvarint(tx *badger.Txn, key []byte) (uint64, error) {
	item, err := tx.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	var ret uint64
	if err := item.Value(func(v []byte) error {
		var n int
		ret, n = binary.Uvarint(v)
		if n <= 0 {
			return errors.New("invalid varint")
		}
		return err
	}); err != nil {
		return 0, err
	}
	return ret, nil
}

func idCompare(a, b cadata.ID) int {
	return bytes.Compare(a[:], b[:])
}
