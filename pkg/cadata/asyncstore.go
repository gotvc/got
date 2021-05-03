package cadata

import (
	"bytes"
	"context"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// AsyncStore allows blobs to be Posted in the background by a pool of workers.
// It is not safe for concurrent use by multiple callers.
//
// Close must return nil for any previous Post to be considered successful.
// When in doubt, call Exists on the underlying store.
//
// If you have a loop that repeatedly calls Post, and Post is high latency, AsyncStore will probably improve performance.
// AsyncStore is not Read-Your-Writes consistent.
type AsyncStore struct {
	target Store
	ctx    context.Context
	eg     *errgroup.Group
	todo   chan *bytes.Buffer
}

func NewAsyncStore(target Store, numWorkers int) *AsyncStore {
	if numWorkers < 1 {
		numWorkers = 1
	}
	eg, ctx := errgroup.WithContext(context.Background())
	as := &AsyncStore{
		target: target,
		ctx:    ctx,
		eg:     eg,
		todo:   make(chan *bytes.Buffer),
	}
	for i := 0; i < numWorkers; i++ {
		as.eg.Go(func() error {
			for buf := range as.todo {
				if err := func() error {
					ctx, cf := context.WithTimeout(ctx, time.Second*1)
					defer cf()
					_, err := as.target.Post(ctx, buf.Bytes())
					return err
				}(); err != nil {
					return err
				}
				buf.Reset()
			}
			return nil
		})
	}
	return as
}

func (s *AsyncStore) Post(ctx context.Context, data []byte) (ID, error) {
	// TODO: error if closed
	buf := &bytes.Buffer{}
	buf.Write(data)
	id := Hash(data)
	select {
	case <-ctx.Done():
		return ID{}, ctx.Err()
	case s.todo <- buf:
	case <-s.ctx.Done():
		return ID{}, errors.Errorf("AsyncStore is closed")
	}
	return id, nil
}

func (s *AsyncStore) GetF(ctx context.Context, id ID, fn func([]byte) error) error {
	return s.target.GetF(ctx, id, fn)
}

func (s *AsyncStore) Delete(ctx context.Context, id ID) error {
	return s.target.Delete(ctx, id)
}

func (s *AsyncStore) Exists(ctx context.Context, id ID) (bool, error) {
	return s.target.Exists(ctx, id)
}

func (s *AsyncStore) List(ctx context.Context, prefix []byte, ids []ID) (int, error) {
	return s.target.List(ctx, prefix, ids)
}

func (s *AsyncStore) Close() error {
	close(s.todo)
	return s.eg.Wait()
}
