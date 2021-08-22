package gotfs

import (
	"bytes"
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/pkg/errors"
)

type (
	ExtentDiffFn   = func(p string, offset uint64, left, right *Extent) error
	MetadataDiffFn = func(p string, left, right *Metadata) error
)

func (o *Operator) Diff(ctx context.Context, s cadata.Store, lRoot, rRoot Root, metaFn MetadataDiffFn, extentFn ExtentDiffFn) error {
	return o.gotkv.Diff(ctx, s, lRoot, rRoot, gotkv.TotalSpan(), func(key, l, r []byte) error {
		if isExtentKey(key) {
			p, offset, err := splitExtentKey(key)
			if err != nil {
				return err
			}
			var lPart, rPart *Extent
			if len(l) > 0 {
				if lPart, err = parseExtent(l); err != nil {
					return err
				}
			}
			if len(r) > 0 {
				if rPart, err = parseExtent(r); err != nil {
					return err
				}
			}
			return extentFn(p, offset, lPart, rPart)
		} else {
			p, err := pathFromKey(key)
			if err != nil {
				return err
			}
			var lMeta, rMeta *Metadata
			if len(l) > 0 {
				if lMeta, err = parseMetadata(l); err != nil {
					return err
				}
			}
			if len(r) > 0 {
				if rMeta, err = parseMetadata(r); err != nil {
					return err
				}
			}
			return metaFn(p, lMeta, rMeta)
		}
	})
}

// DiffPaths calls addFn for additions in next, but not prev, and delFn for paths in prev, but not next.
func (o *Operator) DiffPaths(ctx context.Context, s cadata.Store, prev, next Root, addFn, delFn func(string)) error {
	var lastDelPath, lastAddPath *string
	return o.gotkv.Diff(ctx, s, prev, next, gotkv.TotalSpan(), func(key, leftValue, rightValue []byte) error {
		// deletion
		if rightValue == nil {
			if isExtentKey(key) {
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
	if isExtentKey(key) {
		p, _, err := splitExtentKey(key)
		return p, err
	}
	if bytes.Count(key, []byte{0x00}) > 0 {
		return "", errors.Errorf("gotfs: invalid key %q", key)
	}
	return string(key), nil
}

func Equal(a, b Root) bool {
	return gdat.Equal(a.Ref, b.Ref) &&
		bytes.Equal(a.First, b.First) &&
		a.Depth == b.Depth
}
