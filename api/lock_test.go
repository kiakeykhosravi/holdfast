package api

import (
	"os/exec"
	"path/filepath"
	"testing"
)

// TestRunLockIsExclusive proves two runs cannot proceed at once.
//
// flock is per open file description, so a second Flock from the *same* process
// succeeds and cannot be used to test this. The second acquirer therefore has
// to be a real second process, which is also the case that actually happens:
// cron firing again while the previous run is still going.
func TestRunLockIsExclusive(t *testing.T) {
	dir := t.TempDir()

	unlock, err := AcquireRunLock(dir)
	if err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}

	// flock -n exits 1 when the lock is already held.
	held := exec.Command("flock", "-n", filepath.Join(dir, lockFileName), "true")
	if err := held.Run(); err == nil {
		t.Error("a second process acquired the lock while the first still held it")
	}

	unlock()

	free := exec.Command("flock", "-n", filepath.Join(dir, lockFileName), "true")
	if err := free.Run(); err != nil {
		t.Errorf("lock was not released: %v", err)
	}
}

// TestRunLockCreatesItsDirectory covers a first run on a host where state_dir
// does not exist yet.
func TestRunLockCreatesItsDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "state")
	unlock, err := AcquireRunLock(dir)
	if err != nil {
		t.Fatalf("acquire in a non-existent directory failed: %v", err)
	}
	unlock()
}
