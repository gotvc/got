package staging

import bolt "go.etcd.io/bbolt"

type boltStorage struct {
	db     *bolt.DB
	bucket string
}

func NewBoltStorage(db *bolt.DB, bucket string) Storage {
	return boltStorage{
		db:     db,
		bucket: bucket,
	}
}

func (s boltStorage) Put(k, v []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(s.bucket))
		if err != nil {
			return err
		}
		return b.Put(k, v)
	})
}

func (s boltStorage) Get(k []byte) ([]byte, error) {
	var ret []byte
	if err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.bucket))
		if b == nil {
			return nil
		}
		ret = append([]byte{}, b.Get(k)...)
		return nil
	}); err != nil {
		return nil, err
	}
	return ret, nil
}

func (s boltStorage) ForEach(fn func(k, v []byte) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.bucket))
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if err := fn(k, v); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s boltStorage) Delete(k []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.bucket))
		if b == nil {
			return nil
		}
		return b.Delete(k)
	})
}

func (s boltStorage) DeleteAll() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket([]byte(s.bucket))
		if err == bolt.ErrBucketNotFound {
			return nil
		}
		return err
	})
}
