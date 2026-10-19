package filestore

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"holdfast/interfaces"
	"holdfast/interfaces/storetest"
)

func TestContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) interfaces.Storer {
		s, err := New(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}

func TestNewCreatesDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "a", "b")
	if _, err := New(dir); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		t.Fatalf("directory not created: %v", err)
	}
}

// A writer that is never committed is the signature of a crashed run. Sweep
// must remove it, and the writer, if it somehow resumes, must not be able to
// publish a partial object.
func TestSweepRemovesUncommittedLeftovers(t *testing.T) {
	dir := t.TempDir()
	s, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	w, err := s.Create(ctx, "backup_x.zip.enc")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(dir, ".backup_x.zip.enc.tmp")
	if _, err := os.Stat(tmp); err != nil {
		t.Fatalf("temp file missing before sweep: %v", err)
	}

	if err := s.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("temp file survived sweep: %v", err)
	}
	if err := w.Commit(); err == nil {
		t.Fatal("commit after sweep succeeded; a partial object would be visible")
	}
	got, _ := s.List(ctx, "")
	if len(got) != 0 {
		t.Fatalf("List = %+v, want empty", got)
	}
}

// Sweep must not touch committed objects or files that are not ours.
func TestSweepLeavesCommittedAndForeignFiles(t *testing.T) {
	dir := t.TempDir()
	s, _ := New(dir)
	ctx := context.Background()
	if err := interfaces.PutObject(ctx, s, "backup_y.zip.enc", []byte("done")); err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(dir, "notes.tmp") // no leading dot: not ours
	os.WriteFile(foreign, []byte("keep"), 0o600)
	lock := filepath.Join(dir, ".holdfast.lock") // dotfile but not .tmp
	os.WriteFile(lock, []byte("1"), 0o600)

	if err := s.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{filepath.Join(dir, "backup_y.zip.enc"), foreign, lock} {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("%s removed by sweep", filepath.Base(p))
		}
	}
}

// backup_dir doubles as state_dir when the latter is unset, so the lock file
// and subdirectories can sit next to the archives. They are not objects.
func TestListSkipsDotfilesAndDirectories(t *testing.T) {
	dir := t.TempDir()
	s, _ := New(dir)
	ctx := context.Background()
	os.WriteFile(filepath.Join(dir, ".holdfast.lock"), []byte("1"), 0o600)
	os.MkdirAll(filepath.Join(dir, "state"), 0o700)
	interfaces.PutObject(ctx, s, "backup_z.zip.enc", []byte("z"))

	got, err := s.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Name != "backup_z.zip.enc" {
		t.Fatalf("List = %+v", got)
	}
	if _, err := s.Stat(ctx, "state"); err == nil {
		t.Error("Stat on a directory succeeded")
	}
}
