package gottests

import (
	"os"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (s *Site) AssertFileString(p, expectData string) {
	data, err := s.Root.ReadFile(p)
	require.NoError(s.t, err)
	assert.Equal(s.t, expectData, string(data))
}

func (s *Site) AssertNotExist(p string) {
	_, err := s.Root.Stat(p)
	if !os.IsNotExist(err) {
		s.t.Errorf("expecting file to not exist got %v", err)
	}
}

func (site *Site) AssertFSEquals(m map[string]string) {
	for p, val := range m {
		site.AssertFileString(p, val)
	}
	for p := range site.AllPaths() {
		if _, exists := m[p]; !exists {
			site.t.Errorf("path %s should not exist", p)
		}
	}
}
