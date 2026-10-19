package api

import (
	"context"
	"encoding/json"
	"os"
	"holdfast/types"
	"testing"
	"time"
)

// TestSignedSidecarRoundTrip: a sidecar written with a key reads back with the
// same key and is refused by a different one.
func TestSignedSidecarRoundTrip(t *testing.T) {
	st := newTestStore(t, t.TempDir())
	ctx := context.Background()
	key := MetaKey("a-passphrase-of-adequate-length")
	m := fullMeta("2026-08-25_03-00-00", time.Now())
	if err := WriteMeta(ctx, st, m, key); err != nil {
		t.Fatal(err)
	}
	got, err := ReadMeta(ctx, st, types.MetaFileName(m.ID), key)
	if err != nil {
		t.Fatalf("ReadMeta with the signing key: %v", err)
	}
	if got.Signature == "" {
		t.Fatal("sidecar was written unsigned")
	}
	if _, err := ReadMeta(ctx, st, types.MetaFileName(m.ID), MetaKey("a-different-passphrase-entirely")); err == nil {
		t.Fatal("a sidecar signed under one passphrase verified under another")
	}
}

// TestTamperedSidecarIsSkipped is the point of signing: editing parentID in
// place must make the sidecar untrusted, which stops retention.
func TestTamperedSidecarIsSkipped(t *testing.T) {
	dir := t.TempDir()
	st := newTestStore(t, dir)
	ctx := context.Background()
	key := MetaKey("a-passphrase-of-adequate-length")
	root := fullMeta("2026-08-20_03-00-00", time.Now().Add(-60*24*time.Hour))
	if err := WriteMeta(ctx, st, root, key); err != nil {
		t.Fatal(err)
	}

	// Edit the file the way an attacker with write access to backup_dir would.
	path := metaPath(dir, root.ID)
	raw, _ := os.ReadFile(path)
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	doc["sizeBytes"] = 999999
	edited, _ := json.Marshal(doc)
	if err := os.WriteFile(path, edited, 0o640); err != nil {
		t.Fatal(err)
	}

	scan, err := ReadAllMeta(ctx, st, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(scan.Backups) != 0 || len(scan.Skipped) != 1 {
		t.Fatalf("tampered sidecar should be Skipped, got backups=%d skipped=%d", len(scan.Backups), len(scan.Skipped))
	}
	if _, err := CleanupRetentionStats(ctx, st, 14, key); err == nil {
		t.Fatal("retention ran with a tampered sidecar in the directory")
	}
}

// TestUnsignedSidecarAcceptedWithKey: sidecars from before signing existed
// must keep working, or an upgrade bricks retention on every deployment.
func TestUnsignedSidecarAcceptedWithKey(t *testing.T) {
	st := newTestStore(t, t.TempDir())
	ctx := context.Background()
	m := fullMeta("2026-08-25_03-00-00", time.Now())
	if err := WriteMeta(ctx, st, m, nil); err != nil {
		t.Fatal(err)
	}
	scan, err := ReadAllMeta(ctx, st, MetaKey("a-passphrase-of-adequate-length"))
	if err != nil {
		t.Fatal(err)
	}
	if len(scan.Backups) != 1 || scan.HasSkipped() {
		t.Fatalf("unsigned sidecar must be accepted, got backups=%d skipped=%d", len(scan.Backups), len(scan.Skipped))
	}
	if scan.Unsigned != 1 {
		t.Errorf("Unsigned = %d, want 1", scan.Unsigned)
	}
}
