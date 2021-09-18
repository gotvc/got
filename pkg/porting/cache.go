package porting

import (
	"time"

	"github.com/gotvc/got/pkg/gotfs"
)

type Entry struct {
	ModifiedAt time.Time
	Root       gotfs.Root
}

type Cache interface {
	Put(p string, ent Entry) error
	Get(p string) (*Entry, error)
}
