package api

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"holdfast/types"
)

func fullMeta(id string, at time.Time) types.BackupMeta {
	return types.BackupMeta{
		SchemaVersion:    types.MetaSchemaVersion,
		ID:               id,
		DBType:           "postgres",
		Type:             types.PhysicalFull,
		ChainID:          id,
		CreatedAt:        at,
		EncryptedFile:    "backup_" + id + ".zip.enc",
		SizeBytes:        1024,
		ChecksumSHA256:   "abc123",
		EntryCount:       3,
		Encryption:       types.EncryptionInfo{Container: "DBBK1", KDF: "scrypt"},
		ValidationResult: types.ValidationOK,
	}
}

func incrMeta(id, parentID, chainID string, at time.Time) types.BackupMeta {
	m := fullMeta(id, at)
	m.Type = types.PhysicalIncremental
	m.ParentID = parentID
	m.ChainID = chainID
	return m
}

func TestWriteReadMetaRoundTrip(t *testing.T) {
	st := newTestStore(t, t.TempDir())
	at := time.Date(2026, 8, 25, 3, 0, 0, 0, time.UTC)
	want := fullMeta("2026-08-25_03-00-00", at)

	if err := WriteMeta(context.Background(), st, want, nil); err != nil {
		t.Fatalf("WriteMeta: %v", err)
	}
	got, err := ReadMeta(context.Background(), st, types.MetaFileName(want.ID), nil)
	if err != nil {
		t.Fatalf("ReadMeta: %v", err)
	}
	if got.ID != want.ID || got.Type != want.Type || got.ChainID != want.ChainID {
		t.Errorf("round trip mismatch: got %+v", got)
	}
	if !got.CreatedAt.Equal(want.CreatedAt) {
		t.Errorf("CreatedAt = %v, want %v", got.CreatedAt, want.CreatedAt)
	}
	if !got.IsChainRoot() {
		t.Error("a full backup should be a chain root")
	}
}

// The sidecar is plaintext. If a database name ever reaches it, the whole
// cluster's schema is disclosed to anyone who can list the directory.
func TestMetaDoesNotLeakEntryNames(t *testing.T) {
	dir := t.TempDir()
	m := fullMeta("2026-08-25_03-00-00", time.Now())
	if err := WriteMeta(context.Background(), newTestStore(t, dir), m, nil); err != nil {
		t.Fatalf("WriteMeta: %v", err)
	}
	raw, err := os.ReadFile(metaPath(dir, m.ID))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	for _, forbidden := range []string{"entries", "\"tables\"", ".dump"} {
		if contains(string(raw), forbidden) {
			t.Errorf("sidecar contains %q:\n%s", forbidden, raw)
		}
	}
}

func contains(haystack, needle string) bool {
	return len(needle) > 0 && len(haystack) >= len(needle) &&
		(func() bool {
			for i := 0; i+len(needle) <= len(haystack); i++ {
				if haystack[i:i+len(needle)] == needle {
					return true
				}
			}
			return false
		})()
}

func TestWriteMetaRejectsIncoherent(t *testing.T) {
	st := newTestStore(t, t.TempDir())
	tests := []struct {
		name string
		mut  func(*types.BackupMeta)
	}{
		{"full with a parent", func(m *types.BackupMeta) { m.ParentID = "someone" }},
		{"root with mismatched chainID", func(m *types.BackupMeta) { m.ChainID = "other" }},
		{"missing id", func(m *types.BackupMeta) { m.ID = "" }},
		{"missing createdAt", func(m *types.BackupMeta) { m.CreatedAt = time.Time{} }},
		{"unparseable type", func(m *types.BackupMeta) { m.Type = types.BackupType{Medium: "magical", Level: types.LevelFull} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := fullMeta("2026-08-25_03-00-00", time.Now())
			tt.mut(&m)
			if err := WriteMeta(context.Background(), st, m, nil); err == nil {
				t.Error("want error, got nil")
			}
		})
	}
}

func TestIncrementalWithoutParentIsRejected(t *testing.T) {
	st := newTestStore(t, t.TempDir())
	m := fullMeta("2026-08-25_03-00-00", time.Now())
	m.Type = types.PhysicalIncremental // no ParentID set
	if err := WriteMeta(context.Background(), st, m, nil); err == nil {
		t.Error("an incremental with no parent must not be writable")
	}
}

func TestReadAllMetaSkipsCorruptAndSorts(t *testing.T) {
	dir := t.TempDir()
	st := newTestStore(t, dir)
	base := time.Date(2026, 8, 20, 3, 0, 0, 0, time.UTC)

	root := fullMeta("2026-08-20_03-00-00", base)
	incr1 := incrMeta("2026-08-21_03-00-00", root.ID, root.ID, base.AddDate(0, 0, 1))
	incr2 := incrMeta("2026-08-22_03-00-00", incr1.ID, root.ID, base.AddDate(0, 0, 2))
	other := fullMeta("2026-08-23_03-00-00", base.AddDate(0, 0, 3))

	// Written out of order on purpose — ReadAllMeta must sort.
	for _, m := range []types.BackupMeta{incr2, root, other, incr1} {
		if err := WriteMeta(context.Background(), st, m, nil); err != nil {
			t.Fatalf("WriteMeta %s: %v", m.ID, err)
		}
	}

	// A truncated sidecar, and one from a future schema version.
	if err := os.WriteFile(filepath.Join(dir, "backup_2026-08-24_03-00-00.meta.json"), []byte("{not json"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "backup_2026-08-26_03-00-00.meta.json"),
		[]byte(`{"schemaVersion":99,"id":"2026-08-26_03-00-00"}`), 0o640); err != nil {
		t.Fatal(err)
	}
	// Unrelated files must be ignored entirely.
	if err := os.WriteFile(filepath.Join(dir, "backup_2026-08-20_03-00-00.zip.enc"), []byte("ciphertext"), 0o600); err != nil {
		t.Fatal(err)
	}

	scan, err := ReadAllMeta(context.Background(), st, nil)
	if err != nil {
		t.Fatalf("ReadAllMeta: %v", err)
	}
	if len(scan.Backups) != 4 {
		t.Fatalf("loaded %d backups, want 4", len(scan.Backups))
	}
	if len(scan.Skipped) != 2 {
		t.Errorf("skipped %d, want 2", len(scan.Skipped))
	}
	if !scan.HasSkipped() {
		t.Error("HasSkipped should be true — a deletion pass must refuse to run")
	}

	wantOrder := []string{root.ID, incr1.ID, incr2.ID, other.ID}
	for i, id := range wantOrder {
		if scan.Backups[i].ID != id {
			t.Errorf("position %d = %s, want %s", i, scan.Backups[i].ID, id)
		}
	}

	chains := scan.Chains()
	if len(chains) != 2 {
		t.Fatalf("got %d chains, want 2", len(chains))
	}
	if got := len(chains[root.ID]); got != 3 {
		t.Errorf("chain %s has %d members, want 3", root.ID, got)
	}
	if chains[root.ID][0].ID != root.ID {
		t.Error("chain members must stay oldest-first — that is restore order")
	}
	if got := len(chains[other.ID]); got != 1 {
		t.Errorf("chain %s has %d members, want 1", other.ID, got)
	}
}

func TestReadAllMetaEmptyStore(t *testing.T) {
	scan, err := ReadAllMeta(context.Background(), newTestStore(t, filepath.Join(t.TempDir(), "nope")), nil)
	if err != nil {
		t.Fatalf("an empty store is not an error on first run: %v", err)
	}
	if len(scan.Backups) != 0 || scan.HasSkipped() {
		t.Error("expected an empty scan")
	}
}
