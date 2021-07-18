package ptree

import (
	"bytes"
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/chunking"
	"github.com/gotvc/got/pkg/gdat"
	"golang.org/x/sync/errgroup"
)

type Uploader struct {
	s       cadata.Store
	op      gdat.Operator
	onRef   func(Ref) error
	chunker *chunking.ContentDefined

	todo chan *bytes.Buffer
	err  error
	done chan struct{}
}

func NewUploader(s cadata.Store, numWorkers int, onRef func(Ref) error) *Uploader {
	u := &Uploader{
		s:     s,
		op:    gdat.NewOperator(),
		onRef: onRef,
		todo:  make(chan *bytes.Buffer),
		done:  make(chan struct{}),
	}
	u.chunker = chunking.NewContentDefined(defaultAvgSize, defaultMaxSize, func(data []byte) error {
		buf := &bytes.Buffer{}
		buf.Write(data)
		u.todo <- buf
		return nil
	})
	go func() {
		defer close(u.done)
		u.err = u.coordinator(numWorkers)
	}()
	return u
}

func (u *Uploader) coordinator(numWorkers int) error {
	tasks := make(chan uploadTask)
	resps := make(chan chan Ref, numWorkers)
	eg, ctx := errgroup.WithContext(context.Background())
	// start uploads
	eg.Go(func() error {
		defer close(tasks)
		defer close(resps)
		for buf := range u.todo {
			resp := make(chan Ref)
			tasks <- uploadTask{
				buf:  buf,
				resp: resp,
			}
			resps <- resp
		}
		return nil
	})
	// do uploads
	for i := 0; i < numWorkers; i++ {
		eg.Go(func() error {
			for task := range tasks {
				if err := func() error {
					defer close(task.resp)
					ref, err := u.op.Post(ctx, u.s, task.buf.Bytes())
					if err != nil {
						return err
					}
					task.resp <- *ref
					return nil
				}(); err != nil {
					return err
				}
			}
			return nil
		})
	}
	// wait for uploads
	eg.Go(func() error {
		for resp := range resps {
			ref, ok := <-resp
			if !ok {
				return nil
			}
			if err := u.onRef(ref); err != nil {
				return err
			}
		}
		return nil
	})
	return eg.Wait()
}

func (u *Uploader) Write(p []byte) (int, error) {
	return u.chunker.Write(p)
}

func (u *Uploader) Close() error {
	close(u.todo)
	return nil
}

type uploadTask struct {
	buf  *bytes.Buffer
	resp chan Ref
}
