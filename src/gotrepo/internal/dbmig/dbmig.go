package dbmig

import (
	"embed"
	"io/fs"
	"slices"
	"strings"

	"github.com/gotvc/got/src/internal/migrations"
)

//go:embed *.sql
var migfs embed.FS

func ListMigrations() []migrations.Migration {
	migs, err := loadMigrations()
	if err != nil {
		panic(err)
	}
	return migs
}

func loadMigrations() ([]migrations.Migration, error) {
	ents, err := migfs.ReadDir(".")
	if err != nil {
		return nil, err
	}
	slices.SortFunc(ents, func(a, b fs.DirEntry) int {
		return strings.Compare(a.Name(), b.Name())
	})
	var ret []migrations.Migration
	for i, ent := range ents {
		data, err := migfs.ReadFile(ent.Name())
		if err != nil {
			return nil, err
		}
		ret = append(ret, migrations.Migration{
			RowID:   int64(i + 1),
			Name:    ent.Name(),
			SQLText: string(data),
		})
	}
	return ret, nil
}
