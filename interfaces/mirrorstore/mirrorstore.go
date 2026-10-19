// Package mirrorstore is a Storer that writes to several stores and reads from
// one. It is how backup_dir and a bucket are used together: the directory
// stays the authoritative copy and behaves exactly as it did alone, and the
// bucket receives everything the directory receives.
//
// # Semantics
//
// The primary is authoritative. Open, Stat and List consult only it, so parent
// selection, chain walking and retention decisions see one consistent view.
// Create, Delete and Sweep fan out to every store.
//
// A mirror failing never fails the operation. A backup that exists locally
// but not offsite is strictly better than no backup, and an offsite outage
// should not stop the nightly run. It must not pass silently either: every
// mirror failure is logged when it happens and kept in Failures, and main
// turns a non-empty Failures into a distinct exit code. A primary failure is
// an ordinary failure.
//
// What this is not: a sync. An object the mirror missed stays missing until
// something copies it; the tool does not yet do that.
package mirrorstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"

	"holdfast/interfaces"
	"holdfast/types"
)

// Failure records one mirror operation that did not succeed.
type Failure struct {
	Mirror string // the mirror's Location
	Op     string // "write", "commit", "delete", "sweep"
	Name   string // object name, empty for sweep
	Err    error
}

func (f Failure) String() string {
	if f.Name == "" {
		return fmt.Sprintf("%s: %s: %v", f.Mirror, f.Op, f.Err)
	}
	return fmt.Sprintf("%s: %s %s: %v", f.Mirror, f.Op, f.Name, f.Err)
}

// Store is the primary plus its mirrors.
type Store struct {
	primary interfaces.Storer
	mirrors []interfaces.Storer

	mu       sync.Mutex
	failures []Failure
}

var _ interfaces.Storer = (*Store)(nil)

// New returns primary alone when there are no mirrors, so a single-store
// configuration pays nothing for this package.
func New(primary interfaces.Storer, mirrors ...interfaces.Storer) interfaces.Storer {
	if len(mirrors) == 0 {
		return primary
	}
	return &Store{primary: primary, mirrors: mirrors}
}

// Failures returns every mirror failure recorded so far, in order.
func (s *Store) Failures() []Failure {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Failure(nil), s.failures...)
}

func (s *Store) record(m interfaces.Storer, op, name string, err error) {
	f := Failure{Mirror: m.Location(), Op: op, Name: name, Err: err}
	log.Printf("[mirror] WARNING: %s", f)
	s.mu.Lock()
	s.failures = append(s.failures, f)
	s.mu.Unlock()
}

func (s *Store) Name() string {
	names := []string{s.primary.Name()}
	for _, m := range s.mirrors {
		names = append(names, m.Name())
	}
	return "mirror(" + strings.Join(names, "+") + ")"
}

func (s *Store) Location() string {
	locs := make([]string, 0, len(s.mirrors))
	for _, m := range s.mirrors {
		locs = append(locs, m.Location())
	}
	return s.primary.Location() + " (mirrored to " + strings.Join(locs, ", ") + ")"
}

// Primary exposes the authoritative store.
func (s *Store) Primary() interfaces.Storer { return s.primary }

// Mirrors exposes the mirrors, for the sync mode.
func (s *Store) Mirrors() []interfaces.Storer { return append([]interfaces.Storer(nil), s.mirrors...) }

// Create opens a writer on every store. A mirror that cannot even start is
// recorded and dropped; the primary failing is an error.
func (s *Store) Create(ctx context.Context, name string) (interfaces.ObjectWriter, error) {
	pw, err := s.primary.Create(ctx, name)
	if err != nil {
		return nil, err
	}
	w := &writer{store: s, name: name, primary: pw}
	for _, m := range s.mirrors {
		mw, err := m.Create(ctx, name)
		if err != nil {
			s.record(m, "write", name, err)
			continue
		}
		w.mirrors = append(w.mirrors, mirrorWriter{store: m, w: mw})
	}
	return w, nil
}

type mirrorWriter struct {
	store interfaces.Storer
	w     interfaces.ObjectWriter
}

type writer struct {
	store   *Store
	name    string
	primary interfaces.ObjectWriter
	mirrors []mirrorWriter // only the ones still healthy
}

// Write feeds the primary first; its error is the caller's. Each mirror then
// gets the same bytes; one that fails is aborted, recorded and dropped so a
// dead mirror cannot stall the dump.
func (w *writer) Write(p []byte) (int, error) {
	n, err := w.primary.Write(p)
	if err != nil {
		return n, err
	}
	live := w.mirrors[:0]
	for _, m := range w.mirrors {
		if _, merr := m.w.Write(p); merr != nil {
			w.store.record(m.store, "write", w.name, merr)
			_ = m.w.Abort()
			continue
		}
		live = append(live, m)
	}
	w.mirrors = live
	return n, nil
}

// Commit publishes on the primary and then on each healthy mirror. The
// primary's verdict is the return value; mirror failures are recorded.
func (w *writer) Commit() error {
	if err := w.primary.Commit(); err != nil {
		for _, m := range w.mirrors {
			_ = m.w.Abort()
		}
		w.mirrors = nil
		return err
	}
	for _, m := range w.mirrors {
		if err := m.w.Commit(); err != nil {
			w.store.record(m.store, "commit", w.name, err)
		}
	}
	w.mirrors = nil
	return nil
}

func (w *writer) Abort() error {
	err := w.primary.Abort()
	for _, m := range w.mirrors {
		_ = m.w.Abort()
	}
	w.mirrors = nil
	return err
}

func (s *Store) Open(ctx context.Context, name string) (io.ReadCloser, int64, error) {
	return s.primary.Open(ctx, name)
}

func (s *Store) Stat(ctx context.Context, name string) (types.ObjectInfo, error) {
	return s.primary.Stat(ctx, name)
}

func (s *Store) List(ctx context.Context, prefix string) ([]types.ObjectInfo, error) {
	return s.primary.List(ctx, prefix)
}

// Delete removes from the primary, then from each mirror. An object the
// mirror never had is not a failure: it may have been missed by an earlier
// outage, and retention must still be able to finish.
func (s *Store) Delete(ctx context.Context, name string) error {
	err := s.primary.Delete(ctx, name)
	for _, m := range s.mirrors {
		if merr := m.Delete(ctx, name); merr != nil && !errors.Is(merr, types.ErrObjectNotExist) {
			s.record(m, "delete", name, merr)
		}
	}
	return err
}

func (s *Store) Sweep(ctx context.Context) error {
	err := s.primary.Sweep(ctx)
	for _, m := range s.mirrors {
		if merr := m.Sweep(ctx); merr != nil {
			s.record(m, "sweep", "", merr)
		}
	}
	return err
}
