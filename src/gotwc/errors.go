package gotwc

import "fmt"

type ErrWouldClobber struct {
	Path string
}

func (e ErrWouldClobber) Error() string {
	return fmt.Sprintf("export would clobber path %s", e.Path)
}
