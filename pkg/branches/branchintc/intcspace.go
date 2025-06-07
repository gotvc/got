// Package branchintc implements an interceptor branches.Space which allows function hooks to be added for arbitrary methods.
package branchintc

import (
	"context"

	"go.inet256.org/inet256/pkg/inet256"
)

type PeerID = inet256.Addr

type Verb string

const (
	Verb_Create = "CREATE"
	Verb_Delete = "DELETE"
	Verb_Get    = "GET"
	Verb_Set    = "SET"
	Verb_List   = "LIST"

	Verb_CASCell  = "CAS_CELL"
	Verb_ReadCell = "READ_CELL"

	Verb_GetBlob    = "GET_BLOB"
	Verb_ListBlob   = "LIST_BLOB"
	Verb_ExistsBlob = "EXISTS_BLOB"
	Verb_PostBlob   = "POST_BLOB"
	Verb_DeleteBlob = "DELETE_BLOB"
)

func (v Verb) IsBlob() bool {
	switch v {
	case Verb_GetBlob, Verb_DeleteBlob, Verb_PostBlob, Verb_ExistsBlob, Verb_ListBlob:
		return true
	default:
		return false
	}
}

func (v Verb) IsCell() bool {
	switch v {
	case Verb_CASCell, Verb_ReadCell:
		return true
	default:
		return false
	}
}

func (v Verb) IsBranch() bool {
	switch v {
	case Verb_Create, Verb_Delete, Verb_Get, Verb_Set, Verb_List:
		return true
	default:
		return false
	}
}

type Hook func(ctx context.Context, verb Verb, obj string, next func(ctx context.Context) error) error
