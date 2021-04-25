package cadata

import "context"

type readOnly struct {
	Store
}

func Post(ctx context.Context, data []byte) (ID, error) {
	panic("Post on read only")
}

func Delete(ctx context.Context, id ID) error {
	panic("Delete on read only")
}

// AssertReadOnly returns a new store backup by x, which will panic if it is modified.
func AssertReadOnly(x Store) Store {
	return readOnly{x}
}
