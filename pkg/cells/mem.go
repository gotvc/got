package cells

import (
	"bytes"
	"context"
	"sync"
)

type memCell struct {
	mu    sync.RWMutex
	value []byte
}

func NewMem() Cell {
	return &memCell{}
}

func (mc *memCell) Get(ctx context.Context) ([]byte, error) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return append([]byte{}, mc.value...), nil
}

func (mc *memCell) CAS(ctx context.Context, prev, next []byte) (bool, []byte, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if !bytes.Equal(mc.value, prev) {
		return false, append([]byte{}, mc.value...), nil
	}
	mc.value = append([]byte{}, next...)
	return true, next, nil
}
