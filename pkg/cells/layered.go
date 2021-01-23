package cells

import (
	"context"

	"golang.org/x/sync/errgroup"
)

type layered []CellSpace

// NewLayered creates a layered CellSpace
func NewLayered(css ...CellSpace) CellSpace {
	return layered(css)
}

func (cs layered) ForEach(ctx context.Context, prefix string, fn func(k string) error) error {
	chs := make([]chan string, len(cs))
	for i := range cs {
		chs[i] = make(chan string)
	}
	eg, ctx := errgroup.WithContext(ctx)
	for i := range cs {
		i := i
		eg.Go(func() error {
			defer close(chs[i])
			return cs[i].ForEach(ctx, prefix, func(k string) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case chs[i] <- k:
					return nil
				}
			})
		})
	}
	eg.Go(func() error {
		buf := make([]*string, len(cs))
		for {
			// ensure there is an element from every channel
			for i := range buf {
				if buf[i] == nil {
					if s, ok := <-chs[i]; ok {
						buf[i] = &s
					}
				}
			}
			indexOfNext := -1
			for i := range buf {
				if buf[i] != nil {
					if indexOfNext == -1 || *buf[i] < *buf[indexOfNext] {
						indexOfNext = i
					}
				}
			}
			if indexOfNext == -1 {
				return nil
			}
			if err := fn(*buf[indexOfNext]); err != nil {
				return err
			}
			buf[indexOfNext] = nil
		}
	})
	return eg.Wait()
}

func (cs layered) Get(ctx context.Context, k string) (Cell, error) {
	for i := len(cs) - 1; i >= 0; i-- {
		x, err := cs[i].Get(ctx, k)
		if err == ErrNotExist {
			continue
		} else if err != nil {
			return nil, err
		}
		return x, nil
	}
	return nil, ErrNotExist
}
