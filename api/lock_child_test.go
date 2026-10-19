package api

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// holdLockInChildProcess uses flock(1) to hold the run lock from another
// process for d, then returns once that process has exited.
func holdLockInChildProcess(t *testing.T, dir string, d time.Duration) error {
	t.Helper()
	path := filepath.Join(dir, lockFileName)
	cmd := exec.Command("flock", "-x", path, "sleep", fmt.Sprintf("%.1f", d.Seconds()))
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("flock child: %w", err)
	}
	return nil
}
