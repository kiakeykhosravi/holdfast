package api

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"holdfast/types"
)

// TestRetentionFloorKeepsNewestChains: with three chains all past
// retention_days and min_chains_kept: 2, only the oldest goes. The floor is
// positional, so it is the two newest by their newest member that survive.
func TestRetentionFloorKeepsNewestChains(t *testing.T) {
	dir := t.TempDir()
	oldest := writeBackupSet(t, dir, "2026-01-01_00-00-00", "", "2026-01-01_00-00-00", types.LogicalFull, 90*24*time.Hour)
	middle := writeBackupSet(t, dir, "2026-02-01_00-00-00", "", "2026-02-01_00-00-00", types.LogicalFull, 60*24*time.Hour)
	newest := writeBackupSet(t, dir, "2026-03-01_00-00-00", "", "2026-03-01_00-00-00", types.LogicalFull, 30*24*time.Hour)

	stats, err := CleanupRetentionStats(context.Background(), newTestStore(t, dir), RetentionPolicy{Days: 14, MinChainsKept: 2})
	if err != nil {
		t.Fatalf("CleanupRetentionStats: %v", err)
	}
	if stats.ChainsDeleted != 1 || stats.ChainsKept != 2 || stats.ChainsKeptByFloor != 2 {
		t.Fatalf("stats = %+v, want 1 deleted, 2 kept, 2 by floor", stats)
	}
	if exists(t, filepath.Join(dir, oldest.EncryptedFile)) {
		t.Error("the oldest chain should have been deleted")
	}
	for _, m := range []types.BackupMeta{middle, newest} {
		if !exists(t, filepath.Join(dir, m.EncryptedFile)) || !exists(t, metaPath(dir, m.ID)) {
			t.Errorf("%s was deleted although it is among the %d newest chains", m.ID, 2)
		}
	}
}

// TestRetentionFloorNeverDeletesLastChain is the production default: a host
// whose backups have failed for longer than retention_days keeps its last
// good chain instead of ending up with nothing.
func TestRetentionFloorNeverDeletesLastChain(t *testing.T) {
	dir := t.TempDir()
	base := writeBackupSet(t, dir, "2026-01-01_00-00-00", "", "2026-01-01_00-00-00", types.PhysicalFull, 60*24*time.Hour)
	tip := writeBackupSet(t, dir, "2026-01-02_00-00-00", base.ID, base.ID, types.PhysicalIncremental, 59*24*time.Hour)

	stats, err := CleanupRetentionStats(context.Background(), newTestStore(t, dir), RetentionPolicy{Days: 14, MinChainsKept: 1})
	if err != nil {
		t.Fatalf("CleanupRetentionStats: %v", err)
	}
	if stats.ChainsDeleted != 0 || stats.BackupsDeleted != 0 || stats.ChainsKeptByFloor != 1 {
		t.Fatalf("stats = %+v, want nothing deleted and one chain kept by the floor", stats)
	}
	for _, m := range []types.BackupMeta{base, tip} {
		if !exists(t, filepath.Join(dir, m.EncryptedFile)) {
			t.Errorf("%s was deleted; the last chain must survive", m.ID)
		}
	}
}

// TestRetentionFloorDoesNotCountLiveChains: a chain younger than
// retention_days is kept on age, not by the floor, and the floor's positions
// are consumed by live chains too — the floor is "the N newest", full stop.
func TestRetentionFloorDoesNotCountLiveChains(t *testing.T) {
	dir := t.TempDir()
	expired := writeBackupSet(t, dir, "2026-01-01_00-00-00", "", "2026-01-01_00-00-00", types.LogicalFull, 60*24*time.Hour)
	writeBackupSet(t, dir, "2026-03-01_00-00-00", "", "2026-03-01_00-00-00", types.LogicalFull, 1*time.Hour)

	stats, err := CleanupRetentionStats(context.Background(), newTestStore(t, dir), RetentionPolicy{Days: 14, MinChainsKept: 1})
	if err != nil {
		t.Fatalf("CleanupRetentionStats: %v", err)
	}
	if stats.ChainsDeleted != 1 || stats.ChainsKeptByFloor != 0 {
		t.Fatalf("stats = %+v, want the expired chain deleted and nothing kept by the floor", stats)
	}
	if exists(t, filepath.Join(dir, expired.EncryptedFile)) {
		t.Error("the expired chain survived although a live chain already fills the floor")
	}
}
