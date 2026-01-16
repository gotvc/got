package gottests

import "github.com/stretchr/testify/require"

func (s *Site) AssertFileString(p, expectData string) {
	data, err := s.Root.ReadFile(p)
	require.NoError(s.t, err)
	require.Equal(s.t, expectData, string(data))
}
