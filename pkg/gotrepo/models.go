package gotrepo

import (
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/fs"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/cells"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/gotvc/got/pkg/gotvc"
)

type (
	FS = fs.FS

	Cell   = cells.Cell
	Space  = branches.Space
	Volume = branches.Volume
	Branch = branches.Branch
	Store  = cadata.Store

	Ref  = gotkv.Ref
	Root = gotfs.Root

	Snap = gotvc.Snap
)
