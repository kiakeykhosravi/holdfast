package api

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"holdfast/types"
)

// writeBackupSet lays down the three files a backup consists of.
func writeBackupSet(t *testing.T, dir, id, parentID, chainID string, bt types.BackupType, age time.Duration) types.BackupMeta {
	t.Helper()
	meta := types.BackupMeta{
		SchemaVersion:  types.MetaSchemaVersion,
		ID:             id,
		DBType:         "postgres",
		Type:           bt,
		ChainID:        chainID,
		ParentID:       parentID,
		CreatedAt:      time.Now().Add(-age),
		EncryptedFile:  "backup_" + id + ".zip.enc",
		SizeBytes:      1,
		ChecksumSHA256: "deadbeef",
		EntryCount:     1,
	}
	if err := WriteMeta(context.Background(), newTestStore(t, dir), meta, nil); err != nil {
		t.Fatalf("WriteMeta(%s): %v", id, err)
	}
	enc := filepath.Join(dir, meta.EncryptedFile)
	if err := os.WriteFile(enc, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(enc+".sha256", []byte("deadbeef"), 0o600); err != nil {
		t.Fatal(err)
	}
	return meta
}

func exists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	return err == nil
}

// TestRetentionKeepsChainWithLiveTip is the data-loss regression test.
//
// The old filename-only implementation deleted a physical full as soon as it
// aged out, leaving every incremental built on it present, younger than the
// cutoff, and individually worthless. A chain must survive as a unit for as
// long as any member is still wanted.
func TestRetentionKeepsChainWithLiveTip(t *testing.T) {
	dir := t.TempDir()
	base := writeBackupSet(t, dir, "2026-01-01_00-00-00", "", "2026-01-01_00-00-00", types.PhysicalFull, 30*24*time.Hour)
	tip := writeBackupSet(t, dir, "2026-01-28_00-00-00", base.ID, base.ID, types.PhysicalIncremental, 1*time.Hour)

	stats, err := CleanupRetentionStats(context.Background(), newTestStore(t, dir), RetentionPolicy{Days: 14})
	if err != nil {
		t.Fatalf("CleanupRetentionStats: %v", err)
	}
	if stats.BackupsDeleted != 0 {
		t.Errorf("deleted %d backup(s) from a chain whose tip is still live", stats.BackupsDeleted)
	}
	if stats.ChainsKept != 1 {
		t.Errorf("ChainsKept = %d, want 1", stats.ChainsKept)
	}
	if !exists(t, filepath.Join(dir, base.EncryptedFile)) {
		t.Error("the base was deleted, orphaning its incremental — this is the bug this test exists for")
	}
	if !exists(t, filepath.Join(dir, tip.EncryptedFile)) {
		t.Error("the tip was deleted")
	}
}

// TestRetentionDeletesWholeExpiredChain checks the other half: once no member is
// wanted, every file of every member goes, sidecars included.
func TestRetentionDeletesWholeExpiredChain(t *testing.T) {
	dir := t.TempDir()
	base := writeBackupSet(t, dir, "2026-01-01_00-00-00", "", "2026-01-01_00-00-00", types.PhysicalFull, 60*24*time.Hour)
	mid := writeBackupSet(t, dir, "2026-01-02_00-00-00", base.ID, base.ID, types.PhysicalIncremental, 59*24*time.Hour)

	stats, err := CleanupRetentionStats(context.Background(), newTestStore(t, dir), RetentionPolicy{Days: 14})
	if err != nil {
		t.Fatalf("CleanupRetentionStats: %v", err)
	}
	if stats.ChainsDeleted != 1 || stats.BackupsDeleted != 2 {
		t.Errorf("ChainsDeleted=%d BackupsDeleted=%d, want 1 and 2", stats.ChainsDeleted, stats.BackupsDeleted)
	}
	for _, m := range []types.BackupMeta{base, mid} {
		if exists(t, filepath.Join(dir, m.EncryptedFile)) {
			t.Errorf("%s archive survived", m.ID)
		}
		// The sidecar is the important one: leaving it behind makes
		// ReadAllMeta report a backup whose archive is gone, which breaks
		// parent resolution and chain validation.
		if exists(t, metaPath(dir, m.ID)) {
			t.Errorf("%s sidecar survived, which would poison parent resolution", m.ID)
		}
		if exists(t, filepath.Join(dir, m.EncryptedFile+".sha256")) {
			t.Errorf("%s checksum survived", m.ID)
		}
	}
}

// TestRetentionRefusesWhenASidecarIsUnreadable encodes the MetaScan.Skipped
// contract: an unparseable sidecar describes a backup whose dependents are
// unknown, so nothing may be deleted.
func TestRetentionRefusesWhenASidecarIsUnreadable(t *testing.T) {
	dir := t.TempDir()
	old := writeBackupSet(t, dir, "2026-01-01_00-00-00", "", "2026-01-01_00-00-00", types.LogicalFull, 60*24*time.Hour)
	if err := os.WriteFile(metaPath(dir, "2026-02-02_00-00-00"), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := CleanupRetentionStats(context.Background(), newTestStore(t, dir), RetentionPolicy{Days: 14}); err == nil {
		t.Fatal("expected retention to refuse to run while a sidecar is unreadable")
	}
	if !exists(t, filepath.Join(dir, old.EncryptedFile)) {
		t.Error("retention deleted a backup despite refusing to run")
	}
}

// TestRetentionDeletesUndescribedArchives covers a run that died between the
// archive rename and WriteMeta: no sidecar exists, nothing can reference it, so
// age alone governs.
func TestRetentionDeletesUndescribedArchives(t *testing.T) {
	dir := t.TempDir()
	orphan := filepath.Join(dir, "backup_2026-01-01_00-00-00.zip.enc")
	if err := os.WriteFile(orphan, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(orphan+".sha256", []byte("d"), 0o600); err != nil {
		t.Fatal(err)
	}

	stats, err := CleanupRetentionStats(context.Background(), newTestStore(t, dir), RetentionPolicy{Days: 1})
	if err != nil {
		t.Fatalf("CleanupRetentionStats: %v", err)
	}
	if stats.OrphansDeleted == 0 {
		t.Error("expected the un-described archive to be removed")
	}
	if exists(t, orphan) {
		t.Error("orphan archive survived")
	}
}

// TestRetentionZeroDaysIsANoOp guards the disable switch.
func TestRetentionZeroDaysIsANoOp(t *testing.T) {
	dir := t.TempDir()
	m := writeBackupSet(t, dir, "2020-01-01_00-00-00", "", "2020-01-01_00-00-00", types.LogicalFull, 5000*24*time.Hour)
	if _, err := CleanupRetentionStats(context.Background(), newTestStore(t, dir), RetentionPolicy{Days: 0}); err != nil {
		t.Fatal(err)
	}
	if !exists(t, filepath.Join(dir, m.EncryptedFile)) {
		t.Error("retention_days=0 deleted something")
	}
}
