package porting

import (
	"github.com/gotvc/got/pkg/gotfs"
)

type Entry struct {
	ModifiedAt int64
	Root       gotfs.Root
}
