package api

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"holdfast/types"
)

// TestWriteEncryptedArchiveDoesNotHangOnOutputFailure is a regression test for
// the deadlock that made a full backup mount hang the process instead of
// reporting ENOSPC.
//
// EncryptStream returns on its first failed write without draining the pipe,
// which leaves the dump goroutine blocked in pw.Write forever. The <-done that
// correctly stops the outcome read from racing the goroutine then never
// returns. The fix is to close the read side with the error before waiting.
//
// This test deadlocks the whole package if the fix is reverted, so it is run
// with an explicit timeout rather than relying on the go test deadline.
func TestWriteEncryptedArchiveDoesNotHangOnOutputFailure(t *testing.T) {
	cfg := types.Config{
		BackupDir:         t.TempDir(),
		ScratchDir:        t.TempDir(),
		EncryptPassphrase: "passphrase-long-enough",
	}
	st := newTestStore(t, cfg.BackupDir)
	wantErr := errors.New("no space left on device")
	enc := &failingEncryptor{afterBytes: 1 << 20, err: wantErr}

	type result struct{ err error }
	done := make(chan result, 1)
	go func() {
		// 64 MiB of dump against a 1 MiB failure point: the producer is
		// guaranteed to still be writing when the consumer gives up.
		db := newFakeDB()
		db.entryBytes = 64 << 20
		_, _, err := writeEncryptedArchive(context.Background(), cfg, st, db, enc, types.BackupRequest{Type: types.LogicalFull})
		done <- result{err}
	}()

	select {
	case got := <-done:
		if got.err == nil {
			t.Fatal("expected an error when the output write fails, got nil")
		}
		if !errors.Is(got.err, wantErr) {
			t.Fatalf("expected the underlying write error to survive wrapping, got %v", got.err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("writeEncryptedArchive hung: the producer goroutine was never unblocked")
	}
}

// TestWriteEncryptedArchiveCleansUpOnFailure checks that a failed run leaves no
// partial artefacts behind, since a stray .tmp on the backup mount is invisible
// to retention and an archive with no checksum would be selected as "latest".
func TestWriteEncryptedArchiveCleansUpOnFailure(t *testing.T) {
	dir := t.TempDir()
	cfg := types.Config{
		BackupDir:         dir,
		ScratchDir:        t.TempDir(),
		EncryptPassphrase: "passphrase-long-enough",
	}
	enc := &failingEncryptor{afterBytes: 1 << 20, err: errors.New("disk full")}
	db := newFakeDB()
	db.entryBytes = 8 << 20

	if _, _, err := writeEncryptedArchive(context.Background(), cfg, newTestStore(t, dir), db, enc, types.BackupRequest{Type: types.LogicalFull}); err == nil {
		t.Fatal("expected failure")
	}

	entries, err := osReadDirNames(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected an empty backup dir after a failed run, found %v", entries)
	}
}

var osReadDir = os.ReadDir

func osReadDirNames(dir string) ([]string, error) {
	des, err := osReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, d := range des {
		out = append(out, d.Name())
	}
	return out, nil
}
