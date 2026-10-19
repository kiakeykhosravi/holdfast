package api

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"holdfast/types"
)

// lockFileName is the advisory lock that serialises backup runs on one host.
const lockFileName = ".holdfast.lock"

// LockHeldError means another process currently holds the run lock. It is the
// only lock failure that is a normal outcome: the previous run is still
// working. Every other failure — a state_dir that cannot be created, a lock
// file that cannot be opened — is a fault and must surface as one.
//
// This distinction used to be missing: AcquireRunLock returned a plain error
// for everything and StartBackup turned every error into a skip with exit 0.
// A misconfigured state_dir therefore meant no backup was ever taken while
// every run reported success.
type LockHeldError struct {
	Path string
	// HolderPID is the pid recorded by the holder, 0 if unknown.
	HolderPID int
	// HeldFor is how long the holder has had the lock, 0 if unknown. It comes
	// from the lock file's mtime, which the holder sets when it takes the lock.
	HeldFor time.Duration
}

func (e *LockHeldError) Error() string {
	msg := "another backup run is in progress"
	if e.HolderPID > 0 {
		msg += fmt.Sprintf(" (pid %d)", e.HolderPID)
	}
	if e.HeldFor > 0 {
		msg += fmt.Sprintf(", holding the lock for %s", e.HeldFor.Round(time.Second))
	}
	return msg
}

// LockDir is where the run lock and the parent-state cache live: state_dir,
// falling back to backup_dir when it is unset.
func LockDir(cfg types.Config) string {
	if cfg.StateDir != "" {
		return cfg.StateDir
	}
	return cfg.BackupDir
}

// AcquireRunLock takes an exclusive, non-blocking lock for the duration of a
// backup run and returns the function that releases it.
//
// # Why this is needed
//
// Overlapping runs used to only waste I/O, because every backup was a
// self-contained full. With chains they corrupt bookkeeping: the second run's
// parent resolution reads sidecars while the first is still writing, so it can
// select a tip whose state cache is half-written, or branch the chain by
// rooting two children on the same parent. Physical backups of a large cluster
// run for hours, so a schedule that fires faster than the backup completes
// will hit this rather than theoretically risk it.
//
// # Where the lock lives
//
// In state_dir, not backup_dir. backup_dir is typically an NFS mount, and
// flock over NFS depends on the protocol version and a working lock daemon —
// exactly the sort of thing that fails silently. state_dir is local by
// definition ("disposable local state"), so the lock is reliable there.
//
// The consequence is that this serialises runs *on one host*. Two hosts writing
// to the same shared backup_dir are not coordinated; that needs a lease in
// something that can actually arbitrate, and nothing in this project models it
// yet. One host with overlapping cron is the failure that actually happens.
//
// Contention is reported as *LockHeldError so callers can tell "the previous
// run is still going" apart from "the lock cannot be taken at all". Only the
// former is a skip.
func AcquireRunLock(lockDir string) (func(), error) {
	if lockDir == "" {
		return nil, fmt.Errorf("no directory available for the run lock")
	}
	if err := os.MkdirAll(lockDir, 0o700); err != nil {
		return nil, fmt.Errorf("create lock dir %s: %w", lockDir, err)
	}

	path := filepath.Join(lockDir, lockFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, fmt.Errorf("open lock file %s: %w", path, err)
	}

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("lock %s: %w", path, err)
		}
		held := &LockHeldError{Path: path}
		if b, rerr := os.ReadFile(path); rerr == nil {
			if pid, perr := strconv.Atoi(strings.TrimSpace(string(b))); perr == nil {
				held.HolderPID = pid
			}
		}
		if fi, serr := os.Stat(path); serr == nil {
			held.HeldFor = time.Since(fi.ModTime())
		}
		return nil, held
	}

	// Record the pid for diagnostics. The lock itself is the flock, not this
	// content, so a stale pid in the file is harmless: the kernel drops the
	// flock when the holding process dies, however it dies. Truncating also
	// bumps the mtime, which is how a contender learns how long we have held it.
	if err := f.Truncate(0); err == nil {
		f.WriteAt([]byte(fmt.Sprint(os.Getpid())), 0)
	}

	return func() {
		syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, nil
}
