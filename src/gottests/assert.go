package gottests

import (
	"os"

	"github.com/stretchr/testify/require"
)

func (s *Site) AssertFileString(p, expectData string) {
	data, err := s.Root.ReadFile(p)
	require.NoError(s.t, err)
	require.Equal(s.t, expectData, string(data))
}

func (s *Site) AssertNotExist(p string) {
	_, err := s.Root.Stat(p)
	if !os.IsNotExist(err) {
		s.t.Fatalf("expecting file to not exist got %v", err)
	}
}
