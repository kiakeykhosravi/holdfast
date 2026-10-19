package api

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"holdfast/types"
)

// TestParentWithMissingArchiveIsUnavailable: a sidecar whose archive is gone
// must not be chosen as a parent. With a warm state cache the old code never
// opened the archive and happily built an incremental on a base that no
// longer existed.
func TestParentWithMissingArchiveIsUnavailable(t *testing.T) {
	dir := t.TempDir()
	st := newTestStore(t, dir)
	ctx := context.Background()
	cfg := types.Config{BackupDir: dir, EnableRestoreTest: false}

	root := validated(types.BackupMeta{
		SchemaVersion: types.MetaSchemaVersion, ID: "a", Type: types.PhysicalFull,
		ChainID: "a", CreatedAt: time.Now().Add(-2 * time.Hour), EncryptedFile: "backup_a.zip.enc",
	})
	tip := validated(types.BackupMeta{
		SchemaVersion: types.MetaSchemaVersion, ID: "b", Type: types.PhysicalIncremental,
		ChainID: "a", ParentID: "a", CreatedAt: time.Now().Add(-time.Hour), EncryptedFile: "backup_b.zip.enc",
	})
	for _, m := range []types.BackupMeta{root, tip} {
		if err := WriteMeta(ctx, st, m, nil); err != nil {
			t.Fatal(err)
		}
	}
	// Only the tip's archive exists; the root's was lost.
	if err := os.WriteFile(filepath.Join(dir, tip.EncryptedFile), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, _, err := SelectParentMeta(ctx, cfg, st, types.PhysicalIncremental, "")
	var unavailable *types.IncrementalUnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("missing ancestor archive must be IncrementalUnavailableError so a full is taken; got %T: %v", err, err)
	}

	// An explicit -parent is operator intent, so the same condition is a
	// plain error rather than a silent downgrade.
	_, _, err = SelectParentMeta(ctx, cfg, st, types.PhysicalIncremental, "b")
	if err == nil || errors.As(err, &unavailable) {
		t.Fatalf("explicit -parent with a missing ancestor archive should be a plain error, got %v", err)
	}

	// Once the root archive is back, the tip is usable.
	if err := os.WriteFile(filepath.Join(dir, root.EncryptedFile), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, ok, err := SelectParentMeta(ctx, cfg, st, types.PhysicalIncremental, "")
	if err != nil || !ok || got.ID != "b" {
		t.Fatalf("expected tip b once every archive is present, got ok=%v id=%q err=%v", ok, got.ID, err)
	}
}
