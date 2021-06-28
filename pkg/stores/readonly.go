package stores

import "context"

type readOnly struct {
	Store
}

func (ro readOnly) Post(ctx context.Context, data []byte) (ID, error) {
	panic("Post called on read only Store")
}

func (ro readOnly) Delete(ctx context.Context, id ID) error {
	panic("Delete called on read only Store")
}

// AssertReadOnly returns a new store backup by x, which will panic if it is modified.
func AssertReadOnly(x Store) Store {
	return readOnly{x}
}
