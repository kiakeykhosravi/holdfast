package api

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"holdfast/types"
)

// TestLockContentionIsTyped: only contention may become a skip. The whole
// reason LockHeldError exists is that "cannot create state_dir" used to exit 0
// as a healthy skip, forever, while no backup was ever taken.
func TestLockContentionIsTyped(t *testing.T) {
	dir := t.TempDir()
	unlock, err := AcquireRunLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()

	// A second holder must be another process (flock is per open file
	// description), so simulate contention the way cron produces it.
	done := make(chan error, 1)
	go func() {
		cmdErr := holdLockInChildProcess(t, dir, 2*time.Second)
		done <- cmdErr
	}()
	// The child cannot get it either while we hold it: exercise our own
	// contention path instead by releasing and letting the child take it.
	unlock()
	time.Sleep(300 * time.Millisecond)

	_, err = AcquireRunLock(dir)
	var held *LockHeldError
	if !errors.As(err, &held) {
		t.Fatalf("contention must be *LockHeldError, got %T: %v", err, err)
	}
	if held.HeldFor <= 0 {
		t.Errorf("HeldFor should be positive while another process holds the lock, got %v", held.HeldFor)
	}
	if cerr := <-done; cerr != nil {
		t.Fatal(cerr)
	}
}

// TestLockFaultIsNotContention: an unwritable lock location is a fault and
// must not be reported as "another run is in progress".
func TestLockFaultIsNotContention(t *testing.T) {
	parent := t.TempDir()
	notADir := filepath.Join(parent, "state")
	if err := os.WriteFile(notADir, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := AcquireRunLock(notADir)
	if err == nil {
		t.Fatal("expected an error when the lock dir is a regular file")
	}
	var held *LockHeldError
	if errors.As(err, &held) {
		t.Fatalf("a fault was classified as contention: %v", err)
	}

	// And through acquireOrSkip, the fault must not become a SkipError.
	cfg := types.Config{StateDir: notADir, BackupDir: parent}
	_, err = acquireOrSkip(cfg)
	var skip *SkipError
	if errors.As(err, &skip) {
		t.Fatalf("a lock fault became a healthy skip: %v", err)
	}
}

// TestStuckHolderIsNotASkip: contention that has outlasted any legitimate
// run is a stuck process, and must page rather than skip.
func TestStuckHolderIsNotASkip(t *testing.T) {
	dir := t.TempDir()
	done := make(chan error, 1)
	go func() { done <- holdLockInChildProcess(t, dir, 2*time.Second) }()
	time.Sleep(300 * time.Millisecond)

	// Backdate the lock file so the hold looks ancient.
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(filepath.Join(dir, lockFileName), old, old); err != nil {
		t.Fatal(err)
	}

	cfg := types.Config{StateDir: dir, DumpTimeout: time.Hour, RestoreTimeout: time.Hour}
	_, err := acquireOrSkip(cfg)
	if err == nil {
		t.Fatal("expected an error")
	}
	var skip *SkipError
	if errors.As(err, &skip) {
		t.Fatalf("a lock held for 48h was reported as a healthy skip: %v", err)
	}

	cfg.DumpTimeout, cfg.RestoreTimeout = 0, 0 // no limit configured: plain skip
	_, err = acquireOrSkip(cfg)
	if !errors.As(err, &skip) {
		t.Fatalf("with no timeouts configured contention must be a skip, got %v", err)
	}
	if cerr := <-done; cerr != nil {
		t.Fatal(cerr)
	}
}
