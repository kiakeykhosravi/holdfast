package api

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestRequireSignedRefusesUnsignedSidecar closes the bypass: with
// require_signed_sidecars on, stripping the signature field no longer makes a
// sidecar trusted. It is Skipped, and retention refuses to run.
func TestRequireSignedRefusesUnsignedSidecar(t *testing.T) {
	st := newTestStore(t, t.TempDir())
	ctx := context.Background()
	key := MetaKey("a-passphrase-of-adequate-length")
	m := fullMeta("2026-08-25_03-00-00", time.Now().Add(-60*24*time.Hour))
	if err := WriteMeta(ctx, st, m, nil); err != nil {
		t.Fatal(err)
	}

	scan, err := ReadAllMetaWith(ctx, st, MetaPolicy{Key: key, RequireSigned: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(scan.Backups) != 0 || len(scan.Skipped) != 1 {
		t.Fatalf("unsigned sidecar must be Skipped under RequireSigned, got backups=%d skipped=%d", len(scan.Backups), len(scan.Skipped))
	}
	pol := RetentionPolicy{Days: 14, MinChainsKept: 0, Meta: MetaPolicy{Key: key, RequireSigned: true}}
	if _, err := CleanupRetentionStats(ctx, st, pol); err == nil {
		t.Fatal("retention deleted on the strength of an unsigned sidecar while require_signed_sidecars is on")
	}
}

// TestRequireSignedNeedsAKey: requiring signatures with nothing to verify
// them against must refuse, not silently trust everything.
func TestRequireSignedNeedsAKey(t *testing.T) {
	st := newTestStore(t, t.TempDir())
	_, err := ReadAllMetaWith(context.Background(), st, MetaPolicy{RequireSigned: true})
	if !errors.Is(err, ErrNoMetaKey) {
		t.Fatalf("err = %v, want ErrNoMetaKey", err)
	}
}

// TestSignUnsignedSidecars is the migration: one pass signs every legacy
// sidecar, after which the strict policy accepts them, and a tampered one is
// left alone rather than laundered.
func TestSignUnsignedSidecars(t *testing.T) {
	st := newTestStore(t, t.TempDir())
	ctx := context.Background()
	key := MetaKey("a-passphrase-of-adequate-length")
	for _, id := range []string{"2026-08-20_03-00-00", "2026-08-21_03-00-00"} {
		if err := WriteMeta(ctx, st, fullMeta(id, time.Now()), nil); err != nil {
			t.Fatal(err)
		}
	}
	// One already signed under a different passphrase: tampered as far as
	// this key is concerned.
	if err := WriteMeta(ctx, st, fullMeta("2026-08-22_03-00-00", time.Now()), MetaKey("a-different-passphrase-entirely")); err != nil {
		t.Fatal(err)
	}

	n, err := SignUnsignedSidecars(ctx, st, key)
	if err != nil {
		t.Fatalf("SignUnsignedSidecars: %v", err)
	}
	if n != 2 {
		t.Fatalf("signed %d sidecar(s), want 2", n)
	}

	scan, err := ReadAllMetaWith(ctx, st, MetaPolicy{Key: key, RequireSigned: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(scan.Backups) != 2 || len(scan.Skipped) != 1 {
		t.Fatalf("after signing: backups=%d skipped=%d, want 2 accepted and the foreign-signed one still skipped", len(scan.Backups), len(scan.Skipped))
	}
	if n, err := SignUnsignedSidecars(ctx, st, key); err != nil || n != 0 {
		t.Fatalf("second pass signed %d (err %v), want 0", n, err)
	}
	if n, err := SignUnsignedSidecars(ctx, st, nil); err != nil || n != 0 {
		t.Fatalf("nil key signed %d (err %v), want 0", n, err)
	}
}
