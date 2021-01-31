package gotkv

import "context"

type Iterator struct {
	ctx     context.Context
	s       Store
	root    Ref
	lastKey []byte
}

func NewIterator(ctx context.Context, s Store, x Ref) *Iterator {
	return &Iterator{
		ctx:  ctx,
		s:    s,
		root: x,
	}
}

func (iter *Iterator) SeekPast(key []byte) {
	iter.lastKey = append([]byte{}, key...)
}

func (iter *Iterator) Next(fn func(key, value []byte) error) error {
	return nil
}

func (iter *Iterator) Pos() []byte {
	return iter.lastKey
}
