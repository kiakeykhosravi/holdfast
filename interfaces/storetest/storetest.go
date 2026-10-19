// Package storetest is the behavioural contract every Storer must satisfy. A
// backend's own test calls Run with a constructor; the same assertions then
// hold for a directory and for a bucket, which is what lets api treat them
// interchangeably.
package storetest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"holdfast/interfaces"
	"holdfast/types"
)

// LargeObjectSize is big enough to force a multipart upload on S3 at the
// smallest legal part size (5 MiB), so the streaming path is exercised rather
// than the single-PUT shortcut.
const LargeObjectSize = 6 << 20

// Run executes the contract against fresh stores from open. Each subtest gets
// its own store so they cannot see each other's objects.
func Run(t *testing.T, open func(t *testing.T) interfaces.Storer) {
	t.Helper()
	ctx := context.Background()

	t.Run("LargeRoundTrip", func(t *testing.T) {
		s := open(t)
		want := pattern(LargeObjectSize)
		w, err := s.Create(ctx, "backup_a.zip.enc")
		if err != nil {
			t.Fatal(err)
		}
		// Several writes of uneven size, as an encryptor produces.
		for _, chunk := range split(want, 1<<20+13) {
			if _, err := w.Write(chunk); err != nil {
				t.Fatalf("write: %v", err)
			}
		}
		if err := w.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}

		info, err := s.Stat(ctx, "backup_a.zip.enc")
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		if info.Name != "backup_a.zip.enc" || info.Size != int64(len(want)) {
			t.Fatalf("stat = %+v, want name backup_a.zip.enc size %d", info, len(want))
		}
		if time.Since(info.ModTime) > time.Minute || info.ModTime.IsZero() {
			t.Errorf("mod time %v is not recent", info.ModTime)
		}

		r, size, err := s.Open(ctx, "backup_a.zip.enc")
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer r.Close()
		if size != int64(len(want)) {
			t.Fatalf("open size = %d, want %d", size, len(want))
		}
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		if sha256.Sum256(got) != sha256.Sum256(want) {
			t.Fatalf("content mismatch: got %d bytes", len(got))
		}
	})

	t.Run("AbortLeavesNothing", func(t *testing.T) {
		s := open(t)
		w, err := s.Create(ctx, "backup_b.zip.enc")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(pattern(1 << 20)); err != nil {
			t.Fatal(err)
		}
		if err := w.Abort(); err != nil {
			t.Fatalf("abort: %v", err)
		}
		mustNotExist(t, s, "backup_b.zip.enc")
		mustListEmpty(t, s)
	})

	t.Run("UncommittedIsInvisible", func(t *testing.T) {
		s := open(t)
		w, err := s.Create(ctx, "backup_c.meta.json")
		if err != nil {
			t.Fatal(err)
		}
		defer w.Abort()
		if _, err := w.Write([]byte(`{"id":"c"}`)); err != nil {
			t.Fatal(err)
		}
		mustNotExist(t, s, "backup_c.meta.json")
		mustListEmpty(t, s)
		if err := w.Commit(); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Stat(ctx, "backup_c.meta.json"); err != nil {
			t.Fatalf("after commit: %v", err)
		}
	})

	t.Run("MissingObjects", func(t *testing.T) {
		s := open(t)
		mustNotExist(t, s, "nope")
		if _, _, err := s.Open(ctx, "nope"); !errors.Is(err, types.ErrObjectNotExist) {
			t.Fatalf("open missing: err = %v, want ErrObjectNotExist", err)
		}
		if err := s.Delete(ctx, "nope"); !errors.Is(err, types.ErrObjectNotExist) {
			t.Fatalf("delete missing: err = %v, want ErrObjectNotExist", err)
		}
	})

	t.Run("DeleteExisting", func(t *testing.T) {
		s := open(t)
		put(t, s, "backup_d.zip.enc.sha256", []byte("abc"))
		if err := s.Delete(ctx, "backup_d.zip.enc.sha256"); err != nil {
			t.Fatalf("delete: %v", err)
		}
		mustNotExist(t, s, "backup_d.zip.enc.sha256")
		mustListEmpty(t, s)
	})

	t.Run("ListByPrefix", func(t *testing.T) {
		s := open(t)
		put(t, s, "backup_2.zip.enc", []byte("2"))
		put(t, s, "other.txt", []byte("o"))
		put(t, s, "backup_1.meta.json", []byte("1"))

		got, err := s.List(ctx, "backup_")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 || got[0].Name != "backup_1.meta.json" || got[1].Name != "backup_2.zip.enc" {
			t.Fatalf("List(backup_) = %+v", names(got))
		}
		if got[0].Size != 1 {
			t.Errorf("size = %d, want 1", got[0].Size)
		}
		all, err := s.List(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		if len(all) != 3 {
			t.Fatalf("List() = %v", names(all))
		}
	})

	t.Run("CommitReplaces", func(t *testing.T) {
		s := open(t)
		put(t, s, "backup_e.meta.json", []byte("version one"))
		put(t, s, "backup_e.meta.json", []byte("v2"))
		got, err := interfaces.GetObject(ctx, s, "backup_e.meta.json")
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "v2" {
			t.Fatalf("got %q, want v2", got)
		}
		info, _ := s.Stat(ctx, "backup_e.meta.json")
		if info.Size != 2 {
			t.Fatalf("size after replace = %d, want 2", info.Size)
		}
	})

	t.Run("WriterLifecycle", func(t *testing.T) {
		s := open(t)
		w, err := s.Create(ctx, "backup_f.zip.enc")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		if err := w.Commit(); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte("y")); err == nil {
			t.Error("write after commit succeeded")
		}
		if err := w.Commit(); err == nil {
			t.Error("second commit succeeded")
		}
		if err := w.Abort(); err != nil {
			t.Errorf("abort after commit = %v, want nil (no-op)", err)
		}
		info, err := s.Stat(ctx, "backup_f.zip.enc")
		if err != nil || info.Size != 1 {
			t.Fatalf("object after lifecycle: %+v, %v", info, err)
		}
	})

	t.Run("RejectsBadNames", func(t *testing.T) {
		s := open(t)
		for _, bad := range []string{"", ".", "..", "../x", "a/b", `a\b`, "/abs"} {
			if _, err := s.Create(ctx, bad); err == nil {
				t.Errorf("Create(%q) succeeded", bad)
			}
			if _, err := s.Stat(ctx, bad); err == nil || errors.Is(err, types.ErrObjectNotExist) {
				t.Errorf("Stat(%q) = %v, want a validation error", bad, err)
			}
			if err := s.Delete(ctx, bad); err == nil || errors.Is(err, types.ErrObjectNotExist) {
				t.Errorf("Delete(%q) = %v, want a validation error", bad, err)
			}
		}
		mustListEmpty(t, s)
	})

	t.Run("CancelledContext", func(t *testing.T) {
		s := open(t)
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := s.Create(cctx, "backup_g.zip.enc"); err == nil {
			t.Error("Create with cancelled context succeeded")
		}
		if _, err := s.List(cctx, ""); err == nil {
			t.Error("List with cancelled context succeeded")
		}
		mustListEmpty(t, s)
	})
}

func put(t *testing.T, s interfaces.Storer, name string, data []byte) {
	t.Helper()
	if err := interfaces.PutObject(context.Background(), s, name, data); err != nil {
		t.Fatalf("put %s: %v", name, err)
	}
}

func mustNotExist(t *testing.T, s interfaces.Storer, name string) {
	t.Helper()
	_, err := s.Stat(context.Background(), name)
	if !errors.Is(err, types.ErrObjectNotExist) {
		t.Fatalf("Stat(%s) = %v, want ErrObjectNotExist", name, err)
	}
}

func mustListEmpty(t *testing.T, s interfaces.Storer) {
	t.Helper()
	got, err := s.List(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("List() = %v, want empty", names(got))
	}
}

func names(infos []types.ObjectInfo) []string {
	out := make([]string, len(infos))
	for i, o := range infos {
		out[i] = o.Name
	}
	return out
}

// pattern is deterministic non-constant data so a truncated or reordered
// upload cannot hash the same as the original.
func pattern(n int) []byte {
	b := make([]byte, n)
	var x uint32 = 2463534242
	for i := range b {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		b[i] = byte(x)
	}
	return b
}

func split(b []byte, size int) [][]byte {
	var out [][]byte
	for len(b) > 0 {
		n := min(size, len(b))
		out = append(out, bytes.Clone(b[:n]))
		b = b[n:]
	}
	return out
}

var (
	fakeS3Once sync.Once
	fakeS3URL  string
)

// FakeS3Endpoint starts one in-memory S3 server (gofakes3) for the test
// binary and returns its URL. Any access key is accepted. Callers isolate
// themselves by bucket or prefix, exactly as they would against a shared
// real bucket.
func FakeS3Endpoint(t *testing.T) string {
	t.Helper()
	fakeS3Once.Do(func() {
		srv := httptest.NewServer(gofakes3.New(s3mem.New()).Server())
		fakeS3URL = srv.URL
	})
	return fakeS3URL
}
