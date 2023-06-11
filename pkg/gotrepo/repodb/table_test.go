package repodb

import (
	"testing"

	"github.com/gotvc/got/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCreateTable(t *testing.T) {
	db := testutil.NewTestBadger(t)
	id1, err := GetOrCreateTable(db, "test1")
	require.NoError(t, err)
	require.Equal(t, TableID(1), id1)
	id2, err := GetOrCreateTable(db, "test2")
	require.NoError(t, err)
	require.Equal(t, TableID(2), id2)
	id3, err := GetOrCreateTable(db, "test1")
	require.NoError(t, err)
	require.Equal(t, TableID(1), id3)
}
