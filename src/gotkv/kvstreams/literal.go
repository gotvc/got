package kvstreams

import (
	"bytes"
	"context"
	"sort"

	"go.brendoncarroll.net/exp/streams"
)

// Literal is a stream literal, satisfying the Iterator interface.
// It can be constructed with a slice using NewLiteral
type Literal struct {
	ents []Entry
	pos  int
}

func NewLiteral(xs []Entry) *Literal {
	sort.Slice(xs, func(i, j int) bool {
		return bytes.Compare(xs[i].Key, xs[j].Key) < 0
	})
	return &Literal{ents: xs}
}

func (s *Literal) Next(ctx context.Context, ents []Entry) (int, error) {
	if err := s.Peek(ctx, &ents[0]); err != nil {
		return 0, err
	}
	s.pos++
	return 1, nil
}

func (s *Literal) Peek(ctx context.Context, ent *Entry) error {
	if s.pos >= len(s.ents) {
		return streams.EOS()
	}
	ent.Key = append(ent.Key[:0], s.ents[s.pos].Key...)
	ent.Value = append(ent.Value[:0], s.ents[s.pos].Value...)
	return nil
}

func (s *Literal) Seek(ctx context.Context, gteq []byte) error {
	s.pos = 0
	for s.pos < len(s.ents) {
		if bytes.Compare(s.ents[s.pos].Key, gteq) >= 0 {
			return nil
		}
		s.pos++
	}
	return nil
}
