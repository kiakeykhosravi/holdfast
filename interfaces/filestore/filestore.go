// Package filestore is the Storer backed by a local or mounted directory. It
// is the behaviour backup_dir always had, moved behind the interface without
// weakening any of it.
package filestore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"holdfast/interfaces"
	"holdfast/types"
)

// Store writes objects into one flat directory.
type Store struct {
	dir string
}

var _ interfaces.Storer = (*Store)(nil)

// New creates dir if needed and returns a store over it. The mode matches
// what StartBackup used for backup_dir.
func New(dir string) (*Store, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, fmt.Errorf("filestore: directory is empty")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("filestore: create %s: %w", dir, err)
	}
	return &Store{dir: dir}, nil
}

func (s *Store) Name() string     { return "file" }
func (s *Store) Location() string { return s.dir }

// Dir is the directory this store writes into. Drivers that need a real path
// (none today) can ask for it; api should not.
func (s *Store) Dir() string { return s.dir }

func (s *Store) path(name string) (string, error) {
	if err := types.ValidateObjectName(name); err != nil {
		return "", err
	}
	return filepath.Join(s.dir, name), nil
}

// tempName is the in-progress file for name. It starts with "." so List and
// retention's filename scans never see it, and ends with ".tmp" so Sweep and
// api.SweepStaleTemps recognise it. This is the same pattern
// writeEncryptedArchive and WriteMeta used.
func tempName(name string) string { return "." + name + ".tmp" }

func (s *Store) Create(ctx context.Context, name string) (interfaces.ObjectWriter, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	final, err := s.path(name)
	if err != nil {
		return nil, err
	}
	tmp := filepath.Join(s.dir, tempName(name))
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("filestore: create %s: %w", tmp, err)
	}
	return &writer{f: f, tmp: tmp, final: final, dir: s.dir}, nil
}

// writer is a temp file that becomes the object on Commit.
type writer struct {
	f     *os.File
	tmp   string
	final string
	dir   string
	done  bool
}

var errWriterDone = errors.New("filestore: writer already committed or aborted")

func (w *writer) Write(p []byte) (int, error) {
	if w.done {
		return 0, errWriterDone
	}
	return w.f.Write(p)
}

// Commit is the durability sequence the archive has always had: fsync the
// contents, rename into place, fsync the directory so the rename itself
// survives a crash. Any failure removes the temp file; the final name is
// either the complete object or absent.
func (w *writer) Commit() error {
	if w.done {
		return errWriterDone
	}
	w.done = true
	if err := w.f.Sync(); err != nil {
		w.f.Close()
		os.Remove(w.tmp)
		return fmt.Errorf("filestore: fsync %s: %w", w.tmp, err)
	}
	if err := w.f.Close(); err != nil {
		os.Remove(w.tmp)
		return fmt.Errorf("filestore: close %s: %w", w.tmp, err)
	}
	if err := os.Rename(w.tmp, w.final); err != nil {
		os.Remove(w.tmp)
		return fmt.Errorf("filestore: finalize %s: %w", filepath.Base(w.final), err)
	}
	if err := syncDir(w.dir); err != nil {
		return fmt.Errorf("filestore: fsync directory %s: %w", w.dir, err)
	}
	return nil
}

func (w *writer) Abort() error {
	if w.done {
		return nil
	}
	w.done = true
	w.f.Close()
	if err := os.Remove(w.tmp); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("filestore: remove %s: %w", w.tmp, err)
	}
	return nil
}

func (s *Store) Open(ctx context.Context, name string) (io.ReadCloser, int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	p, err := s.path(name)
	if err != nil {
		return nil, 0, err
	}
	f, err := os.Open(p)
	if err != nil {
		return nil, 0, wrapNotExist(name, err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, fi.Size(), nil
}

func (s *Store) Stat(ctx context.Context, name string) (types.ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return types.ObjectInfo{}, err
	}
	p, err := s.path(name)
	if err != nil {
		return types.ObjectInfo{}, err
	}
	fi, err := os.Stat(p)
	if err != nil {
		return types.ObjectInfo{}, wrapNotExist(name, err)
	}
	if fi.IsDir() {
		return types.ObjectInfo{}, fmt.Errorf("%s: %w", name, types.ErrObjectNotExist)
	}
	return types.ObjectInfo{Name: name, Size: fi.Size(), ModTime: fi.ModTime()}, nil
}

// List skips directories and dotfiles: in-progress temp files, the lock file
// when state_dir falls back to backup_dir, and the dry-run probe all start
// with ".".
func (s *Store) List(ctx context.Context, prefix string) ([]types.ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []types.ObjectInfo
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || strings.HasPrefix(n, ".") || !strings.HasPrefix(n, prefix) {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			if os.IsNotExist(err) {
				continue // removed between ReadDir and Info
			}
			return nil, err
		}
		out = append(out, types.ObjectInfo{Name: n, Size: fi.Size(), ModTime: fi.ModTime()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (s *Store) Delete(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p, err := s.path(name)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil {
		return wrapNotExist(name, err)
	}
	return nil
}

// Sweep removes every ".<name>.tmp" in the directory. Under the run lock no
// writer on this host is live, so each one is the leftover of a crashed run.
func (s *Store) Sweep(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var errs []error
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasPrefix(n, ".") || !strings.HasSuffix(n, ".tmp") {
			continue
		}
		if err := os.Remove(filepath.Join(s.dir, n)); err != nil && !os.IsNotExist(err) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func wrapNotExist(name string, err error) error {
	if os.IsNotExist(err) {
		return fmt.Errorf("%s: %w", name, types.ErrObjectNotExist)
	}
	return err
}

// syncDir fsyncs a directory so that renames inside it are durable. Without it
// a crash can lose the directory entry for a file whose contents were flushed.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
