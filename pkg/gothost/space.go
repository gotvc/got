package gothost

import (
	"context"
	"fmt"

	"github.com/gotvc/got/pkg/branches"
)

type Space struct {
	branches.Space
}

func (s Space) Create(ctx context.Context, k string, md branches.Metadata) (*branches.Branch, error) {
	if k == HostConfigKey {
		return nil, newConfigBranchErr()
	}
	return s.Space.Create(ctx, k, md)
}

func (s Space) Set(ctx context.Context, k string, md branches.Metadata) error {
	if k == HostConfigKey {
		return newConfigBranchErr()
	}
	return s.Space.Set(ctx, k, md)
}

func (s Space) Delete(ctx context.Context, k string) error {
	if k == HostConfigKey {
		return newConfigBranchErr()
	}
	return s.Space.Delete(ctx, k)
}

func newConfigBranchErr() error {
	return fmt.Errorf("cannot delete %s branch", HostConfigKey)
}
