package gotrepo

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"

	"github.com/brendoncarroll/go-state/posixfs"
)

func loadIgnore(ctx context.Context, fs posixfs.FS, p string) (func(x string) bool, error) {
	data, err := posixfs.ReadFile(ctx, fs, p)
	if posixfs.IsErrNotExist(err) {
		return func(string) bool { return false }, nil
	} else if err != nil {
		return nil, err
	}
	return parseIgnore(data)
}

func parseIgnore(data []byte) (func(x string) bool, error) {
	var pats []string
	lines := bytes.Split(data, []byte("\n"))
	for i, line := range lines {
		if len(line) == 0 {
			continue
		}
		pat := string(line)
		if _, err := filepath.Match(pat, ""); err != nil {
			return nil, fmt.Errorf("gotignore has bad pattern %q on line %d", pat, i)
		}
		pats = append(pats, pat)
	}
	return func(x string) bool {
		for _, pat := range pats {
			match, err := filepath.Match(pat, x)
			if err != nil {
				panic(err)
			}
			if match {
				return true
			}
		}
		return false
	}, nil
}
