package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"holdfast/interfaces"
	"holdfast/types"
)

// syncFixture is a primary holding two complete backup sets and an empty
// mirror, both file-backed.
type syncFixture struct {
	cfg             types.Config
	primaryDir      string
	primary, mirror interfaces.Storer
}

func newSyncFixture(t *testing.T) *syncFixture {
	t.Helper()
	f := &syncFixture{primaryDir: t.TempDir()}
	f.primary = newTestStore(t, f.primaryDir)
	f.mirror = newTestStore(t, t.TempDir())
	f.cfg = types.Config{BackupDir: f.primaryDir, StateDir: t.TempDir()}
	writeBackupSetWithPayload(t, f.primaryDir, "2026-01-01_00-00-00", "", "2026-01-01_00-00-00", types.PhysicalFull, make([]byte, 2<<20))
	writeBackupSetWithPayload(t, f.primaryDir, "2026-01-02_00-00-00", "2026-01-01_00-00-00", "2026-01-01_00-00-00", types.PhysicalIncremental, []byte("delta"))
	return f
}

// writeBackupSetWithPayload is writeBackupSet with a real archive body and a
// matching checksum, so copyObject's integrity check has something to verify.
func writeBackupSetWithPayload(t *testing.T, dir, id, parentID, chainID string, bt types.BackupType, payload []byte) types.BackupMeta {
	t.Helper()
	sum := sha256.Sum256(payload)
	meta := types.BackupMeta{
		SchemaVersion: types.MetaSchemaVersion, ID: id, DBType: "postgres", Type: bt,
		ChainID: chainID, ParentID: parentID, CreatedAt: time.Now().Add(-time.Hour),
		EncryptedFile: archiveName(id), SizeBytes: int64(len(payload)),
		ChecksumSHA256: hex.EncodeToString(sum[:]), EntryCount: 1,
	}
	if err := WriteMeta(context.Background(), newTestStore(t, dir), meta, nil); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, meta.EncryptedFile), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, checksumName(meta.EncryptedFile)), []byte(meta.ChecksumSHA256), 0o600); err != nil {
		t.Fatal(err)
	}
	return meta
}

func names(objs []types.ObjectInfo) []string {
	out := make([]string, len(objs))
	for i, o := range objs {
		out[i] = o.Name
	}
	return out
}

func TestSyncCopiesEverythingThenIsIdempotent(t *testing.T) {
	ctx := context.Background()
	f := newSyncFixture(t)

	res, err := SyncMirrors(ctx, f.cfg, f.primary, []interfaces.Storer{f.mirror}, SyncOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Failed != 0 || len(res.Sources[0].Mirrors) != 1 || res.Sources[0].Mirrors[0].Copied != 6 || res.Sources[0].Mirrors[0].UpToDate != 0 {
		t.Fatalf("first sync = %+v", res.Sources[0].Mirrors[0])
	}
	want, _ := f.primary.List(ctx, "")
	got, _ := f.mirror.List(ctx, "")
	if len(want) != 6 || len(got) != 6 {
		t.Fatalf("primary %v, mirror %v", names(want), names(got))
	}
	for i := range want {
		if want[i].Name != got[i].Name || want[i].Size != got[i].Size {
			t.Fatalf("object %d differs: %+v vs %+v", i, want[i], got[i])
		}
	}

	res, err = SyncMirrors(ctx, f.cfg, f.primary, []interfaces.Storer{f.mirror}, SyncOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Sources[0].Mirrors[0].Copied != 0 || res.Sources[0].Mirrors[0].UpToDate != 6 {
		t.Fatalf("second sync should copy nothing: %+v", res.Sources[0].Mirrors[0])
	}
}

// A sidecar rewritten in place with the same length (a validation verdict
// toggled) must still be re-copied: small objects compare by content.
func TestSyncRecopiesChangedSidecarOfSameSize(t *testing.T) {
	ctx := context.Background()
	f := newSyncFixture(t)
	if _, err := SyncMirrors(ctx, f.cfg, f.primary, []interfaces.Storer{f.mirror}, SyncOptions{}); err != nil {
		t.Fatal(err)
	}
	metaName := types.MetaFileName("2026-01-01_00-00-00")
	b, _ := interfaces.GetObject(ctx, f.primary, metaName)
	b[len(b)-3] ^= 0x01 // same length, different byte (inside the trailing whitespace/brace area is fine for this test)
	if err := interfaces.PutObject(ctx, f.primary, metaName, b); err != nil {
		t.Fatal(err)
	}
	res, err := SyncMirrors(ctx, f.cfg, f.primary, []interfaces.Storer{f.mirror}, SyncOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Sources[0].Mirrors[0].Copied != 1 {
		t.Fatalf("changed sidecar was not re-copied: %+v", res.Sources[0].Mirrors[0])
	}
}

func TestSyncRefusesToReplicateCorruptArchive(t *testing.T) {
	ctx := context.Background()
	f := newSyncFixture(t)
	// Flip a byte in the primary's archive; its .sha256 no longer matches.
	p := filepath.Join(f.primaryDir, archiveName("2026-01-02_00-00-00"))
	if err := os.WriteFile(p, []byte("delta-corrupted"), 0o600); err != nil {
		t.Fatal(err)
	}
	res, err := SyncMirrors(ctx, f.cfg, f.primary, []interfaces.Storer{f.mirror}, SyncOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Failed != 1 || len(res.Sources[0].Mirrors[0].Failed) != 1 {
		t.Fatalf("expected exactly one failure for the corrupt archive, got %+v", res.Sources[0].Mirrors[0])
	}
	if _, err := f.mirror.Stat(ctx, archiveName("2026-01-02_00-00-00")); !errors.Is(err, types.ErrObjectNotExist) {
		t.Fatalf("corrupt archive reached the mirror: %v", err)
	}
	// Everything else still went.
	if res.Sources[0].Mirrors[0].Copied != 5 {
		t.Fatalf("copied %d, want 5", res.Sources[0].Mirrors[0].Copied)
	}
}

func TestSyncPruneIsOptInAndGuarded(t *testing.T) {
	ctx := context.Background()
	f := newSyncFixture(t)
	if _, err := SyncMirrors(ctx, f.cfg, f.primary, []interfaces.Storer{f.mirror}, SyncOptions{}); err != nil {
		t.Fatal(err)
	}
	// Retention on the backup host removed the incremental.
	for _, n := range []string{archiveName("2026-01-02_00-00-00"), checksumName(archiveName("2026-01-02_00-00-00")), types.MetaFileName("2026-01-02_00-00-00")} {
		if err := f.primary.Delete(ctx, n); err != nil {
			t.Fatal(err)
		}
	}

	// Without -prune the mirror keeps it.
	res, _ := SyncMirrors(ctx, f.cfg, f.primary, []interfaces.Storer{f.mirror}, SyncOptions{})
	if res.Sources[0].Mirrors[0].Pruned != 0 {
		t.Fatal("pruned without being asked")
	}
	if got, _ := f.mirror.List(ctx, ""); len(got) != 6 {
		t.Fatalf("mirror = %v", names(got))
	}

	// With -prune it goes.
	res, _ = SyncMirrors(ctx, f.cfg, f.primary, []interfaces.Storer{f.mirror}, SyncOptions{Prune: true})
	if res.Sources[0].Mirrors[0].Pruned != 3 || res.Sources[0].PruneRefused != "" {
		t.Fatalf("prune = %+v, refused=%q", res.Sources[0].Mirrors[0], res.Sources[0].PruneRefused)
	}
	if got, _ := f.mirror.List(ctx, ""); len(got) != 3 {
		t.Fatalf("mirror after prune = %v", names(got))
	}

	// An empty primary looks like an unmounted share: prune must refuse.
	empty := newTestStore(t, t.TempDir())
	res, err := SyncMirrors(ctx, f.cfg, empty, []interfaces.Storer{f.mirror}, SyncOptions{Prune: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Sources[0].PruneRefused == "" || res.Sources[0].Mirrors[0].Pruned != 0 {
		t.Fatalf("prune against an empty primary was not refused: %+v", res)
	}
	if got, _ := f.mirror.List(ctx, ""); len(got) != 3 {
		t.Fatalf("mirror was emptied: %v", names(got))
	}

	// An untrusted primary sidecar also blocks pruning.
	if err := os.WriteFile(metaPath(f.primaryDir, "2026-03-03_00-00-00"), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	res, _ = SyncMirrors(ctx, f.cfg, f.primary, []interfaces.Storer{f.mirror}, SyncOptions{Prune: true})
	if res.Sources[0].PruneRefused == "" {
		t.Fatal("prune proceeded with an unreadable primary sidecar")
	}
}

func TestSyncOrdersArchivesBeforeSidecars(t *testing.T) {
	objs := []types.ObjectInfo{
		{Name: "backup_b.meta.json"}, {Name: "backup_a.zip.enc.sha256"}, {Name: "backup_a.meta.json"}, {Name: "backup_b.zip.enc"},
	}
	sortForCopy(objs)
	got := names(objs)
	want := []string{"backup_a.zip.enc.sha256", "backup_b.zip.enc", "backup_a.meta.json", "backup_b.meta.json"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("copy order = %v, want %v", got, want)
		}
	}
	sortForPrune(objs)
	if !isMetaName(objs[0].Name) || !isMetaName(objs[1].Name) {
		t.Fatalf("prune order = %v, want sidecars first", names(objs))
	}
}

func TestSyncRequiresAMirror(t *testing.T) {
	f := newSyncFixture(t)
	if _, err := SyncMirrors(context.Background(), f.cfg, f.primary, nil, SyncOptions{}); err == nil {
		t.Fatal("sync with no mirrors succeeded")
	}
}

// Several sources run in one locked pass, each to its own mirror; a source
// that cannot be listed is reported and does not stop the others.
func TestSyncSourcesAreIndependentAndAggregated(t *testing.T) {
	ctx := context.Background()
	a := newSyncFixture(t)
	b := newSyncFixture(t)
	broken := SyncSource{Name: "broken", Err: errors.New("bucket unreachable")}

	res, err := SyncSources(ctx, a.cfg, []SyncSource{
		{Name: "a", Primary: a.primary, Mirrors: []interfaces.Storer{a.mirror}},
		broken,
		{Name: "b", Primary: b.primary, Mirrors: []interfaces.Storer{b.mirror}},
	}, SyncOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Sources) != 3 {
		t.Fatalf("sources = %d, want 3", len(res.Sources))
	}
	if res.Sources[0].Name != "a" || res.Sources[0].Mirrors[0].Copied != 6 {
		t.Fatalf("source a = %+v", res.Sources[0])
	}
	if res.Sources[1].Name != "broken" || res.Sources[1].Error == "" || res.Sources[1].Failed != 1 {
		t.Fatalf("broken source = %+v", res.Sources[1])
	}
	if res.Sources[2].Name != "b" || res.Sources[2].Mirrors[0].Copied != 6 {
		t.Fatalf("source b ran after a failure? %+v", res.Sources[2])
	}
	if res.Failed != 1 {
		t.Fatalf("Failed = %d, want 1", res.Failed)
	}
	// Each mirror holds only its own source's objects.
	ga, _ := a.mirror.List(ctx, "")
	gb, _ := b.mirror.List(ctx, "")
	if len(ga) != 6 || len(gb) != 6 {
		t.Fatalf("mirror a %d objects, mirror b %d", len(ga), len(gb))
	}
}
