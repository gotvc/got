package testutil

import "io/fs"

type MemFile struct {
	Mode fs.FileMode
	Data []byte
}

type MemFS = map[string]MemFile
