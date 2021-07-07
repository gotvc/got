package gotrepo

import (
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/got/pkg/branches"
	"github.com/brendoncarroll/got/pkg/cells"
	"github.com/brendoncarroll/got/pkg/fs"
	"github.com/brendoncarroll/got/pkg/gotfs"
	"github.com/brendoncarroll/got/pkg/gotkv"
	"github.com/brendoncarroll/got/pkg/gotvc"
)

type (
	FS = fs.FS

	Cell   = cells.Cell
	Realm  = branches.Realm
	Volume = branches.Volume
	Branch = branches.Branch
	Store  = cadata.Store

	Ref  = gotkv.Ref
	Root = gotfs.Root

	Commit = gotvc.Snapshot
)
