package cadata

import (
	"context"

	"github.com/blobcache/blobcache/pkg/blobs"
	"golang.org/x/sync/errgroup"
)

type (
	Store  = blobs.Store
	Lister = blobs.Lister
	Getter = blobs.Getter
	Poster = blobs.Poster
)

var (
	ErrNotFound = blobs.ErrNotFound
)

type ID = blobs.ID

func Hash(data []byte) blobs.ID {
	return blobs.Hash(data)
}

func IDFromBytes(x []byte) ID {
	id := ID{}
	copy(id[:], x)
	return id
}

func NewMem() *blobs.MemStore {
	return blobs.NewMem()
}

func ForEach(ctx context.Context, s blobs.Lister, fn func(id ID) error) error {
	return blobs.ForEach(ctx, s, fn)
}

type Pinner interface {
	Poster
	// Pin allows store to add the data by ID alone.
	// It will return ErrNotFound if the caller should call Post instead.
	Pin(ctx context.Context, id ID) error
}

func Copy(ctx context.Context, dst blobs.Poster, src blobs.Getter, id ID) error {
	if pinner, ok := dst.(Pinner); ok {
		if err := pinner.Pin(ctx, id); err != ErrNotFound {
			return err
		}
	}
	return src.GetF(ctx, id, func(data []byte) error {
		_, err := dst.Post(ctx, data)
		return err
	})
}

type CopyAllFrom interface {
	CopyAllFrom(ctx context.Context, src Store) error
}

func CopyAll(ctx context.Context, dst, src Store) error {
	if caf, ok := dst.(CopyAllFrom); ok {
		return caf.CopyAllFrom(ctx, src)
	}
	return CopyAllBasic(ctx, dst, src)
}

func CopyAllBasic(ctx context.Context, dst, src Store) error {
	const numWorkers = 16
	ch := make(chan blobs.ID)
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return ForEach(ctx, src, func(id ID) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ch <- id:
			}
			return nil
		})
	})
	for i := 0; i < numWorkers; i++ {
		eg.Go(func() error {
			for id := range ch {
				if err := Copy(ctx, src, dst, id); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return eg.Wait()
}

// DeleteAll deletes all the data in s
func DeleteAll(ctx context.Context, s Store) error {
	return ForEach(ctx, s, func(id ID) error {
		return s.Delete(ctx, id)
	})
}

type Set interface {
	Exists(ctx context.Context, id ID) (bool, error)
	Add(ctx context.Context, id ID) error
}

type MemSet map[ID]struct{}

func (ms MemSet) Exists(ctx context.Context, id ID) (bool, error) {
	_, exists := ms[id]
	return exists, nil
}

func (ms MemSet) Add(ctx context.Context, id ID) error {
	ms[id] = struct{}{}
	return nil
}

func (ms MemSet) Count() int {
	return len(ms)
}
