package e2etest

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/gotvc/got/src/gotcmd"
	"github.com/stretchr/testify/require"
)

var _ = gotcmd.Main // rerun these tests when gotcmd changes

func TestCmd(t *testing.T) {
	execPath := goBuild(t, "../..", "./cmd/got")

	// Init checks that got init && got status runs without errors
	t.Run("Init", func(t *testing.T) {
		tempDir := t.TempDir()
		runCmd(t, tempDir, execPath, "init")
		runCmd(t, tempDir, execPath, "status")
	})
}

func runCmd(t testing.TB, workDir string, execPath string, args ...string) {
	cmd := exec.CommandContext(t.Context(), execPath, args...)
	cmd.Dir = workDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Run())
}

func goBuild(t testing.TB, workDir, mainPath string) string {
	outPath := filepath.Join(t.TempDir(), "main-bin")
	cmd := exec.Command("go", "build",
		"-o", outPath,
		mainPath)
	cmd.Dir = workDir
	cmd.Env = []string{}
	for _, key := range []string{
		"GOPATH",
		"GOCACHE",
		"GOROOT",
		"HOME",
	} {
		if val := os.Getenv(key); val != "" {
			cmd.Env = append(cmd.Env, key+"="+val)
		}
	}
	cmdOut, err := cmd.CombinedOutput()
	if len(cmdOut) != 0 {
		t.Log("cmd out: ", string(cmdOut))
	}
	require.NoError(t, err)
	return outPath
}
