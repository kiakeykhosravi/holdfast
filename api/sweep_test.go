package api

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"holdfast/types"
)

// TestSweepRemovesOnlyLeftovers: a crashed run's temp archive and scratch
// directories go; real backups and unrelated files stay.
func TestSweepRemovesOnlyLeftovers(t *testing.T) {
	backupDir := t.TempDir()
	scratch := t.TempDir()
	cfg := types.Config{BackupDir: backupDir, ScratchDir: scratch}

	stale := filepath.Join(backupDir, ".backup_2026-01-01_00-00-00.zip.enc.tmp")
	staleMeta := filepath.Join(backupDir, ".backup_2026-01-01_00-00-00.meta.json.tmp")
	real := filepath.Join(backupDir, "backup_2026-01-01_00-00-00.zip.enc")
	for _, p := range []string{stale, staleMeta, real} {
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	staleDir := filepath.Join(scratch, "holdfast_basebackup_123")
	otherDir := filepath.Join(scratch, "someone-elses-dir")
	for _, d := range []string{staleDir, otherDir} {
		if err := os.MkdirAll(filepath.Join(d, "inner"), 0o700); err != nil {
			t.Fatal(err)
		}
	}

	SweepStaleTemps(context.Background(), cfg, newTestStore(t, backupDir))

	for _, gone := range []string{stale, staleMeta, staleDir} {
		if _, err := os.Stat(gone); err == nil {
			t.Errorf("%s should have been swept", gone)
		}
	}
	for _, kept := range []string{real, otherDir} {
		if _, err := os.Stat(kept); err != nil {
			t.Errorf("%s should have been left alone", kept)
		}
	}
}
