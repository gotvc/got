package kvstreams

import (
	"bytes"
	"context"
	"io"

	"github.com/brendoncarroll/go-state/cadata"
)

var _ Iterator = &Merger{}

type Merger struct {
	streams []Iterator
}

func NewMerger(s cadata.Store, streams []Iterator) *Merger {
	return &Merger{
		streams: streams,
	}
}

func (sm *Merger) Next(ctx context.Context, ent *Entry) error {
	sr, err := sm.selectStream(ctx)
	if err != nil {
		return err
	}
	if err := sr.Next(ctx, ent); err != nil {
		return err
	}
	if err != nil {
		return err
	}
	return sm.advancePast(ctx, ent.Key)
}

func (sm *Merger) Peek(ctx context.Context, ent *Entry) error {
	sr, err := sm.selectStream(ctx)
	if err != nil {
		return err
	}
	return sr.Peek(ctx, ent)
}

func (m *Merger) Seek(ctx context.Context, gteq []byte) error {
	for i := range m.streams {
		if err := m.streams[i].Seek(ctx, gteq); err != nil {
			return err
		}
	}
	return nil
}

func (sm *Merger) advancePast(ctx context.Context, key []byte) error {
	var ent Entry
	for _, sr := range sm.streams {
		if err := sr.Peek(ctx, &ent); err != nil {
			if err == EOS {
				continue
			}
			return err
		}
		// if the stream is behind, advance it.
		if bytes.Compare(ent.Key, key) <= 0 {
			if err := sr.Next(ctx, &ent); err != nil {
				return err
			}
		}
	}
	return nil
}

// selectStream will never return an ended stream
func (sm *Merger) selectStream(ctx context.Context) (Iterator, error) {
	var minKey []byte
	nextIndex := len(sm.streams)
	var ent Entry
	for i, sr := range sm.streams {
		if err := sr.Peek(ctx, &ent); err != nil {
			if err == EOS {
				continue
			}
			return nil, err
		}
		if minKey == nil || bytes.Compare(ent.Key, minKey) <= 0 {
			minKey = ent.Key
			nextIndex = i
		}
	}
	if nextIndex < len(sm.streams) {
		return sm.streams[nextIndex], nil
	}
	return nil, io.EOF
}
