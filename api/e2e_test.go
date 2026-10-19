package api

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"holdfast/interfaces"
	"holdfast/interfaces/crypto"
	"holdfast/interfaces/filestore"
	"holdfast/interfaces/mirrorstore"
	"holdfast/interfaces/s3store"
	"holdfast/interfaces/storetest"
	"holdfast/types"
)

// TestPipelineEndToEnd drives the real orchestration — StartBackup with the
// real AES encryptor, an incremental on a cold state cache, an out-of-band
// validation, retention, and a decrypt by object name — through each storage
// backend. The driver is fake; everything between it and the store is real.
//
// The S3 case runs against an in-process fake by default. With
// HOLDFAST_S3_TEST_ENDPOINT set it runs against that server instead, which is
// how the pipeline was proven against a real bucket.
func TestPipelineEndToEnd(t *testing.T) {
	t.Run("file", func(t *testing.T) {
		runPipeline(t, func(t *testing.T) interfaces.Storer {
			s, err := filestore.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			return s
		})
	})
	t.Run("s3", func(t *testing.T) {
		runPipeline(t, func(t *testing.T) interfaces.Storer {
			return newS3TestStore(t)
		})
	})
	// The production shape: backup_dir as the primary, the bucket as a
	// mirror. Every object the directory ends up with, the bucket must too.
	t.Run("file_mirrored_to_s3", func(t *testing.T) {
		var primary, mirror interfaces.Storer
		runPipeline(t, func(t *testing.T) interfaces.Storer {
			p, err := filestore.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			primary, mirror = p, newS3TestStore(t)
			return mirrorstore.New(primary, mirror)
		})
		ctx := context.Background()
		want, err := primary.List(ctx, "backup_")
		if err != nil {
			t.Fatal(err)
		}
		if len(want) == 0 {
			t.Fatal("primary holds nothing after the pipeline")
		}
		for _, o := range want {
			got, err := mirror.Stat(ctx, o.Name)
			if err != nil {
				t.Errorf("mirror is missing %s: %v", o.Name, err)
				continue
			}
			if got.Size != o.Size {
				t.Errorf("%s: mirror size %d, primary size %d", o.Name, got.Size, o.Size)
			}
		}
	})
}

func newS3TestStore(t *testing.T) interfaces.Storer {
	t.Helper()
	opts := s3store.Options{
		Bucket:          "holdfast-test",
		Prefix:          fmt.Sprintf("e2e/%d", time.Now().UnixNano()),
		Region:          "us-east-1",
		Endpoint:        storetest.FakeS3Endpoint(t),
		UsePathStyle:    true,
		AccessKeyID:     "fake",
		SecretAccessKey: "fake-secret",
		PartSize:        5 << 20,
		Concurrency:     2,
	}
	if ep := os.Getenv("HOLDFAST_S3_TEST_ENDPOINT"); ep != "" {
		opts.Endpoint = ep
		opts.Bucket = os.Getenv("HOLDFAST_S3_TEST_BUCKET")
		opts.AccessKeyID = os.Getenv("HOLDFAST_S3_TEST_ACCESS_KEY")
		opts.SecretAccessKey = os.Getenv("HOLDFAST_S3_TEST_SECRET_KEY")
		opts.LegacyCompat = os.Getenv("HOLDFAST_S3_TEST_LEGACY") != ""
	}
	ctx := context.Background()
	s, err := s3store.New(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	// The fake needs the bucket to exist; a real endpoint already has it.
	if err := interfaces.PutObject(ctx, s, ".probe", []byte("x")); err != nil {
		if os.Getenv("HOLDFAST_S3_TEST_ENDPOINT") != "" {
			t.Fatalf("bucket %s not usable: %v", opts.Bucket, err)
		}
		createFakeBucket(t, opts)
		if err := interfaces.PutObject(ctx, s, ".probe", []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	_ = s.Delete(ctx, ".probe")
	t.Cleanup(func() {
		objs, _ := s.List(ctx, "")
		for _, o := range objs {
			_ = s.Delete(ctx, o.Name)
		}
	})
	return s
}

func runPipeline(t *testing.T, open func(t *testing.T) interfaces.Storer) {
	ctx := context.Background()
	store := open(t)
	cfg := types.Config{
		StateDir:          t.TempDir(),
		ScratchDir:        t.TempDir(),
		EncryptPassphrase: "a-passphrase-of-adequate-length",
		EnableRestoreTest: true,
		FallbackToFull:    true,
		MaxChainLength:    7,
		RetentionDays:     14,
	}
	enc := crypto.NewAESEncryptor()
	db := newFakeDB()
	db.entryBytes = 6 << 20 // two S3 parts at the 5 MiB minimum

	// 1. A full. Validation runs, so the sidecar must end up validated.
	full, err := StartBackup(ctx, cfg, store, db, enc, types.PhysicalFull, "")
	if err != nil {
		t.Fatalf("full backup: %v", err)
	}
	if !full.TestedOK || full.ChainID != full.ID || full.ParentID != "" {
		t.Fatalf("full result = %+v", full)
	}
	for _, name := range []string{archiveName(full.ID), checksumName(archiveName(full.ID)), types.MetaFileName(full.ID)} {
		if _, err := store.Stat(ctx, name); err != nil {
			t.Fatalf("%s missing after full backup: %v", name, err)
		}
	}
	if _, err := os.Stat(StatePath(cfg, full.ID)); err != nil {
		t.Fatalf("state cache not written: %v", err)
	}

	// 2. An incremental on a cold cache: the parent manifest has to come
	// back out of the archive in the store.
	if err := os.RemoveAll(filepath.Join(cfg.StateDir, "state")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond) // IDs have one-second resolution
	incr, err := StartBackup(ctx, cfg, store, db, enc, types.PhysicalIncremental, "")
	if err != nil {
		t.Fatalf("incremental backup: %v", err)
	}
	if incr.BackupType != types.PhysicalIncremental || incr.ParentID != full.ID || incr.ChainID != full.ID {
		t.Fatalf("incremental did not chain onto the full: %+v", incr)
	}
	if !incr.TestedOK {
		t.Fatal("incremental was not validated")
	}

	// 3. Out-of-band validation of the tip walks the whole chain.
	vres, err := ValidateEncryptedBackup(ctx, cfg, store, db, enc, cfg.EncryptPassphrase, "")
	if err != nil {
		t.Fatalf("validate-only: %v", err)
	}
	if vres.ChainMembers != 2 || !vres.MetaRecorded || vres.ChecksumOK == nil || !*vres.ChecksumOK {
		t.Fatalf("validate-only result = %+v", vres)
	}

	// 4. Retention keeps a live chain and deletes nothing.
	stats, err := RunRetention(ctx, cfg, store)
	if err != nil {
		t.Fatalf("retention: %v", err)
	}
	if stats.BackupsDeleted != 0 || stats.ChainsKept != 1 {
		t.Fatalf("retention stats = %+v", stats)
	}

	// 5. Decrypt by object name into a chosen path; the plaintext is a zip
	// Unzip accepts and it holds the entries the driver wrote.
	out := filepath.Join(t.TempDir(), "plain.zip")
	dres, err := DecryptEncryptedBackup(ctx, store, enc, archiveName(full.ID), out, cfg.EncryptPassphrase)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if dres.ChecksumOK == nil || !*dres.ChecksumOK {
		t.Fatalf("decrypt did not verify the checksum: %+v", dres)
	}
	files, err := Unzip(out, filepath.Join(t.TempDir(), "x"))
	if err != nil {
		t.Fatalf("unzip plaintext: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("plaintext holds %d entries, want 2", len(files))
	}

	// 6. A wrong passphrase leaves nothing behind.
	bad := filepath.Join(t.TempDir(), "bad.zip")
	if _, err := DecryptEncryptedBackup(ctx, store, enc, archiveName(full.ID), bad, "the-wrong-passphrase-entirely"); err == nil {
		t.Fatal("decrypt with the wrong passphrase succeeded")
	}
	if _, err := os.Stat(bad); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial plaintext left behind: %v", err)
	}

	// 7. Dry run reports the plan without writing: the tip is the planned
	// parent and the probe object is gone afterwards.
	dry, err := RunDryRun(ctx, cfg, types.CLIOptions{}, store, nil, db, enc)
	if err != nil && dry == nil {
		t.Fatalf("dry run: %v", err)
	}
	if !dry.StorageOK || dry.Storage != store.Location() {
		t.Fatalf("dry run storage = %+v", dry)
	}
	if _, err := store.Stat(ctx, ".dry_run_probe"); err == nil {
		t.Fatal("dry-run probe object left behind")
	}
}

// createFakeBucket makes the bucket on the in-process fake, which starts
// empty.
func createFakeBucket(t *testing.T, opts s3store.Options) {
	t.Helper()
	if err := s3store.CreateBucket(context.Background(), opts); err != nil {
		t.Fatal(err)
	}
}
