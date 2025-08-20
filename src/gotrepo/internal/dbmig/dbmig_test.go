package dbmig

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListMigrations(t *testing.T) {
	migs, err := loadMigrations()
	require.NoError(t, err)
	for _, mig := range migs {
		t.Log(mig)
	}
	if len(migs) < 1 {
		t.Errorf("migrations not loaded")
	}
}
