package mirrorstore

import (
	"context"
	"errors"
	"io"
	"testing"

	"holdfast/interfaces"
	"holdfast/interfaces/filestore"
	"holdfast/interfaces/storetest"
	"holdfast/types"
)

func newFile(t *testing.T) *filestore.Store {
	t.Helper()
	s, err := filestore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// A mirror of two healthy stores must satisfy the same contract as one store.
func TestContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) interfaces.Storer {
		return New(newFile(t), newFile(t))
	})
}

func TestNoMirrorsReturnsPrimary(t *testing.T) {
	p := newFile(t)
	if got := New(p); got != interfaces.Storer(p) {
		t.Fatalf("New(primary) = %T, want the primary itself", got)
	}
}

func TestWritesReachBothAndReadsComeFromPrimary(t *testing.T) {
	ctx := context.Background()
	p, m := newFile(t), newFile(t)
	s := New(p, m)

	if err := interfaces.PutObject(ctx, s, "backup_a.zip.enc", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	for _, st := range []interfaces.Storer{p, m} {
		got, err := interfaces.GetObject(ctx, st, "backup_a.zip.enc")
		if err != nil || string(got) != "payload" {
			t.Fatalf("%s: %q, %v", st.Location(), got, err)
		}
	}

	// Something only the mirror has is invisible: the primary is the truth.
	if err := interfaces.PutObject(ctx, m, "backup_mirror_only.zip.enc", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Stat(ctx, "backup_mirror_only.zip.enc"); !errors.Is(err, types.ErrObjectNotExist) {
		t.Fatalf("Stat of a mirror-only object = %v, want ErrObjectNotExist", err)
	}
	objs, _ := s.List(ctx, "")
	if len(objs) != 1 {
		t.Fatalf("List = %v, want only the primary's object", objs)
	}

	if err := s.Delete(ctx, "backup_a.zip.enc"); err != nil {
		t.Fatal(err)
	}
	for _, st := range []interfaces.Storer{p, m} {
		if _, err := st.Stat(ctx, "backup_a.zip.enc"); !errors.Is(err, types.ErrObjectNotExist) {
			t.Errorf("%s still has the object after Delete: %v", st.Location(), err)
		}
	}
	if f := s.(*Store).Failures(); len(f) != 0 {
		t.Fatalf("unexpected failures: %v", f)
	}
}

// The reason this package exists: an offsite outage must not cost the local
// backup, and must not go unnoticed.
func TestMirrorFailureIsRecordedNotFatal(t *testing.T) {
	ctx := context.Background()
	p := newFile(t)
	bad := &failingStore{Storer: newFile(t), failWriteAfter: 10, err: errors.New("connection reset")}
	s := New(p, bad).(*Store)

	w, err := s.Create(ctx, "backup_b.zip.enc")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, err := w.Write(make([]byte, 8)); err != nil {
			t.Fatalf("write %d: primary must not see the mirror's failure: %v", i, err)
		}
	}
	if err := w.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	info, err := p.Stat(ctx, "backup_b.zip.enc")
	if err != nil || info.Size != 32 {
		t.Fatalf("primary object: %+v, %v", info, err)
	}
	if _, err := bad.Storer.Stat(ctx, "backup_b.zip.enc"); !errors.Is(err, types.ErrObjectNotExist) {
		t.Fatalf("failed mirror must hold no partial object, got %v", err)
	}
	f := s.Failures()
	if len(f) != 1 || f[0].Op != "write" || f[0].Name != "backup_b.zip.enc" {
		t.Fatalf("Failures = %v", f)
	}
}

func TestPrimaryFailureIsFatalAndAbortsMirrors(t *testing.T) {
	ctx := context.Background()
	bad := &failingStore{Storer: newFile(t), failWriteAfter: 10, err: errors.New("disk full")}
	m := newFile(t)
	s := New(bad, m)

	w, err := s.Create(ctx, "backup_c.zip.enc")
	if err != nil {
		t.Fatal(err)
	}
	var werr error
	for i := 0; i < 4 && werr == nil; i++ {
		_, werr = w.Write(make([]byte, 8))
	}
	if werr == nil {
		t.Fatal("primary failure was swallowed")
	}
	_ = w.Abort()
	if _, err := m.Stat(ctx, "backup_c.zip.enc"); !errors.Is(err, types.ErrObjectNotExist) {
		t.Fatalf("mirror published an object the primary never committed: %v", err)
	}
}

func TestDeleteToleratesMissingOnMirror(t *testing.T) {
	ctx := context.Background()
	p, m := newFile(t), newFile(t)
	s := New(p, m).(*Store)
	// Written to the primary only, as if the mirror had been down.
	if err := interfaces.PutObject(ctx, p, "backup_d.zip.enc", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "backup_d.zip.enc"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if f := s.Failures(); len(f) != 0 {
		t.Fatalf("a missing mirror object is not a failure, got %v", f)
	}
}

// failingStore wraps a real store and makes writers fail after N bytes.
type failingStore struct {
	interfaces.Storer
	failWriteAfter int
	err            error
}

func (f *failingStore) Create(ctx context.Context, name string) (interfaces.ObjectWriter, error) {
	w, err := f.Storer.Create(ctx, name)
	if err != nil {
		return nil, err
	}
	return &failingWriter{ObjectWriter: w, limit: f.failWriteAfter, err: f.err}, nil
}

type failingWriter struct {
	interfaces.ObjectWriter
	written int
	limit   int
	err     error
}

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.written+len(p) > w.limit {
		return 0, w.err
	}
	w.written += len(p)
	return w.ObjectWriter.Write(p)
}

var _ io.Writer = (*failingWriter)(nil)
