package testutil

type MemFile struct {
	Mode uint32
	Data []byte
}

type MemFS = map[string]MemFile
