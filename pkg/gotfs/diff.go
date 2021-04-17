package gotfs

import (
	"bytes"
	"context"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/gotkv"
	"github.com/pkg/errors"
)

// DiffPaths calls addFn for additions in next, but not prev, and delFn for paths in prev, but not next.
func (o *Operator) DiffPaths(ctx context.Context, s cadata.Store, prev, next Root, addFn, delFn func(string)) error {
	var lastDelPath, lastAddPath *string
	return o.gotkv.Diff(ctx, s, prev, next, gotkv.TotalSpan(), func(key, leftValue, rightValue []byte) error {
		// deletion
		if rightValue == nil {
			if isPartKey(key) {
				return nil
			}
			p := string(key)
			if lastDelPath == nil || p != *lastDelPath {
				delFn(p)
				lastDelPath = &p
			}
			return nil
		}
		// addition
		p, err := pathFromKey(key)
		if err != nil {
			return err
		}
		if lastAddPath == nil || p != *lastAddPath {
			addFn(p)
			lastAddPath = &p
		}
		return nil
	})
}

func pathFromKey(key []byte) (string, error) {
	if isPartKey(key) {
		p, _, err := splitPartKey(key)
		return p, err
	}
	if bytes.Count(key, []byte{0x00}) > 0 {
		return "", errors.Errorf("gotfs: invalid key %q", key)
	}
	return string(key), nil
}
