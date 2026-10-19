package api

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"holdfast/types"
)

func validated(m types.BackupMeta) types.BackupMeta {
	now := time.Now()
	m.ValidatedAt = &now
	m.ValidationResult = types.ValidationOK
	return m
}

// TestChainRolloverIsRecoverable is the regression test for the defect that
// bricked scheduled incrementals.
//
// Every one of these conditions arises on a healthy system during normal
// operation, and the right response to each is to take a full backup. They used
// to be plain errors, which main turned into exit 2 with no backup taken at
// all — on every subsequent run, until a human intervened. config.yaml's
// promise that max_chain_length "forces a new full" depends on this.
func TestChainRolloverIsRecoverable(t *testing.T) {
	base := validated(types.BackupMeta{
		SchemaVersion: types.MetaSchemaVersion, ID: "a", Type: types.PhysicalFull,
		ChainID: "a", CreatedAt: time.Now().Add(-2 * time.Hour), EncryptedFile: "backup_a.zip.enc",
	})
	inc := validated(types.BackupMeta{
		SchemaVersion: types.MetaSchemaVersion, ID: "b", Type: types.PhysicalIncremental,
		ChainID: "a", ParentID: "a", CreatedAt: time.Now().Add(-time.Hour), EncryptedFile: "backup_b.zip.enc",
	})

	cases := []struct {
		name string
		cfg  types.Config
		scan MetaScan
	}{
		{
			name: "no backups at all",
			cfg:  types.Config{BackupDir: "/b", EnableRestoreTest: true},
			scan: MetaScan{},
		},
		{
			name: "no backup of the requested medium",
			cfg:  types.Config{BackupDir: "/b", EnableRestoreTest: true},
			scan: MetaScan{Backups: []types.BackupMeta{validated(types.BackupMeta{
				SchemaVersion: types.MetaSchemaVersion, ID: "l", Type: types.LogicalFull,
				ChainID: "l", CreatedAt: time.Now(), EncryptedFile: "backup_l.zip.enc",
			})}},
		},
		{
			name: "chain at max_chain_length",
			cfg:  types.Config{BackupDir: "/b", MaxChainLength: 2, EnableRestoreTest: true},
			scan: MetaScan{Backups: []types.BackupMeta{base, inc}},
		},
		{
			name: "tip failed its restore test",
			cfg:  types.Config{BackupDir: "/b", EnableRestoreTest: true},
			scan: MetaScan{Backups: []types.BackupMeta{base, func() types.BackupMeta {
				m := inc
				m.ValidationResult = types.ValidationFailed
				return m
			}()}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newestUsableChainMember(tc.scan, tc.cfg, types.PhysicalIncremental)
			if err == nil {
				t.Fatal("expected an error")
			}
			var unavailable *types.IncrementalUnavailableError
			if !errors.As(err, &unavailable) {
				t.Fatalf("error must be IncrementalUnavailableError so fallback_to_full can downgrade it; got %T: %v", err, err)
			}
		})
	}
}

// TestUnvalidatedTipAcceptedWhenRestoreTestsDisabled covers the coupling that
// made incrementals outright impossible with enable_restore_test: false.
//
// Nothing ever sets validationResult in that mode, so every tip looks unproven
// and demanding proof is incoherent rather than cautious.
func TestUnvalidatedTipAcceptedWhenRestoreTestsDisabled(t *testing.T) {
	tip := types.BackupMeta{
		SchemaVersion: types.MetaSchemaVersion, ID: "a", Type: types.PhysicalFull,
		ChainID: "a", CreatedAt: time.Now(), EncryptedFile: "backup_a.zip.enc",
	}
	cfg := types.Config{BackupDir: "/b", EnableRestoreTest: false}

	got, err := newestUsableChainMember(MetaScan{Backups: []types.BackupMeta{tip}}, cfg, types.PhysicalIncremental)
	if err != nil {
		t.Fatalf("with restore tests disabled an unvalidated tip must be usable, got %v", err)
	}
	if got.ID != "a" {
		t.Fatalf("got tip %q, want %q", got.ID, "a")
	}

	// With restore tests on, the same tip must be refused.
	cfg.EnableRestoreTest = true
	if _, err := newestUsableChainMember(MetaScan{Backups: []types.BackupMeta{tip}}, cfg, types.PhysicalIncremental); err == nil {
		t.Fatal("with restore tests enabled an unvalidated tip must be refused")
	}
}

// TestMediumFilterPicksSameMediumTip checks that a directory holding both
// schedules does not root a physical incremental on a logical dump.
func TestMediumFilterPicksSameMediumTip(t *testing.T) {
	physical := validated(types.BackupMeta{
		SchemaVersion: types.MetaSchemaVersion, ID: "p", Type: types.PhysicalFull,
		ChainID: "p", CreatedAt: time.Now().Add(-time.Hour), EncryptedFile: "backup_p.zip.enc",
	})
	logical := validated(types.BackupMeta{
		SchemaVersion: types.MetaSchemaVersion, ID: "l", Type: types.LogicalFull,
		ChainID: "l", CreatedAt: time.Now(), EncryptedFile: "backup_l.zip.enc",
	})

	got, err := newestUsableChainMember(
		MetaScan{Backups: []types.BackupMeta{physical, logical}},
		types.Config{BackupDir: "/b", EnableRestoreTest: true},
		types.PhysicalIncremental,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "p" {
		t.Fatalf("picked %q; the newest backup overall is logical and must be skipped", got.ID)
	}
}

// TestResolveLineageDetectsCycle guards the chain walker against a sidecar set
// that points at itself, which would otherwise loop forever.
func TestResolveLineageDetectsCycle(t *testing.T) {
	dir := t.TempDir()
	// Written directly: WriteMeta's Validate would reject these on purpose.
	writeRawMeta(t, dir, `{"schemaVersion":1,"id":"a","type":"physical-incremental","chainID":"root","parentID":"b","createdAt":"2026-01-01T00:00:00Z","encryptedFile":"backup_a.zip.enc"}`, "a")
	writeRawMeta(t, dir, `{"schemaVersion":1,"id":"b","type":"physical-incremental","chainID":"root","parentID":"a","createdAt":"2026-01-01T00:00:00Z","encryptedFile":"backup_b.zip.enc"}`, "b")

	st := newTestStore(t, dir)
	leaf, err := ReadMeta(context.Background(), st, types.MetaFileName("a"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolveLineage(context.Background(), st, MetaPolicy{}, leaf); err == nil {
		t.Fatal("expected a cycle to be reported rather than looping")
	}
}

// TestResolveLineageReportsMissingAncestor: a pruned ancestor must produce a
// clear error, not a silently truncated chain that validates as if complete.
func TestResolveLineageReportsMissingAncestor(t *testing.T) {
	dir := t.TempDir()
	writeRawMeta(t, dir, `{"schemaVersion":1,"id":"b","type":"physical-incremental","chainID":"a","parentID":"a","createdAt":"2026-01-01T00:00:00Z","encryptedFile":"backup_b.zip.enc"}`, "b")

	st := newTestStore(t, dir)
	leaf, err := ReadMeta(context.Background(), st, types.MetaFileName("b"), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = resolveLineage(context.Background(), st, MetaPolicy{}, leaf)
	if err == nil {
		t.Fatal("expected a missing-parent error")
	}
}

func writeRawMeta(t *testing.T, dir, body, id string) {
	t.Helper()
	if err := os.WriteFile(metaPath(dir, id), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}
