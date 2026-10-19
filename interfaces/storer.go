package interfaces

import (
	"bytes"
	"context"
	"io"

	"holdfast/types"
)

// Storer is where finished backup artefacts live: the encrypted archive, its
// .sha256 and its .meta.json sidecar. It abstracts backup_dir and nothing
// else.
//
// # What it deliberately does not cover
//
// state_dir (run lock, parent-manifest cache) and scratch_dir (pg_basebackup
// staging, chain extraction) stay on local disk. The lock is an flock, which
// has no meaning in object storage, and scratch holds cluster-sized temporary
// trees that drivers hand to pg_combinebackup by path. Putting either behind
// this interface would be a lie about what they are.
//
// # Contract
//
// Names are flat basenames (types.ValidateObjectName). A Storer never
// interprets them: chain walking, retention, and "what is a backup" are api's
// business, driven by the sidecars, exactly as before.
//
// Create returns a writer whose bytes are invisible until Commit. A writer
// that is abandoned, aborted, or fails leaves no object under its name. This
// is the property writeEncryptedArchive and WriteMeta currently get from
// "temp file, fsync, rename", and both backends must provide it natively: the
// file store with that exact sequence, S3 with a multipart upload that is only
// completed on Commit.
//
// Commit on an existing name replaces the object. api already refuses to
// overwrite an archive by checking Stat first; WriteMeta on the other hand
// rewrites a sidecar on purpose when it records a validation result.
//
// Open returns the object's size alongside its reader because the decryptor
// needs to know where the HMAC trailer starts before it reads the stream.
//
// Delete reports types.ErrObjectNotExist when nothing was there, so retention
// can keep counting what it actually removed.
//
// Sweep removes the leftovers of writers that were never committed or aborted
// (a crashed run). It is only safe under the run lock, for the same reason
// SweepStaleTemps is.
type Storer interface {
	// Name identifies the backend, e.g. "file" or "s3".
	Name() string

	// Location is a human-readable description of where objects go, for log
	// and error messages: a directory path or an s3://bucket/prefix URL.
	Location() string

	Create(ctx context.Context, name string) (ObjectWriter, error)
	Open(ctx context.Context, name string) (io.ReadCloser, int64, error)
	Stat(ctx context.Context, name string) (types.ObjectInfo, error)

	// List returns every committed object whose name starts with prefix,
	// sorted by name. Uncommitted writers are never listed.
	List(ctx context.Context, prefix string) ([]types.ObjectInfo, error)

	Delete(ctx context.Context, name string) error

	Sweep(ctx context.Context) error
}

// ObjectWriter is an in-progress object. Exactly one of Commit or Abort ends
// it; after either, Write fails. Abort after Commit is a no-op, so callers can
// defer Abort unconditionally and Commit on the happy path.
type ObjectWriter interface {
	io.Writer

	// Commit makes the object visible under its name and durable: when it
	// returns nil the bytes will survive a crash of this process and, for the
	// file backend, a power loss.
	Commit() error

	// Abort discards everything written. It never fails because of what was
	// written; it only reports a problem cleaning up.
	Abort() error
}

// PutObject writes a small object in one call. For the archive itself use
// Create and stream; this is for the checksum and the sidecar.
func PutObject(ctx context.Context, s Storer, name string, data []byte) error {
	w, err := s.Create(ctx, name)
	if err != nil {
		return err
	}
	if _, err := io.Copy(w, bytes.NewReader(data)); err != nil {
		_ = w.Abort()
		return err
	}
	return w.Commit()
}

// GetObject reads a whole object into memory. Only for small objects.
func GetObject(ctx context.Context, s Storer, name string) ([]byte, error) {
	r, _, err := s.Open(ctx, name)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}
