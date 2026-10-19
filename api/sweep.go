package api

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"

	"holdfast/interfaces"
	"holdfast/types"
)

// scratchPrefix is the common prefix of every temporary directory this tool
// creates in scratch_dir. SweepStaleTemps relies on it, so a new MkdirTemp
// pattern anywhere in the codebase must start with it.
const scratchPrefix = "holdfast_"

// SweepStaleTemps removes the leftovers of runs that died without unwinding:
// an OOM kill, a SIGKILL, a host reboot mid-dump.
//
// # Why this exists
//
// An archive is invisible until its writer commits, and a crashed run never
// commits. For the file backend that is a cluster-sized ".backup_<id>.tmp"
// that retention never looks at; for S3 it is an incomplete multipart upload
// that is billed for its parts indefinitely. The Storer's Sweep knows which.
// The same went for the staging directories in scratch_dir: every path creates
// them with os.MkdirTemp and removes them with a deferred RemoveAll, which a
// hard kill never reaches. On a host that runs physical backups, two or three
// such leftovers are enough to fill the disk.
//
// # Why it must run under the run lock
//
// Every directory matched here could belong to a live run. The run lock is
// what proves no other run exists on this host, so the sweep is only safe
// immediately after acquiring it, and every mode that creates temp state
// takes the lock for that reason.
//
// Failures are logged, never returned: a sweep that cannot remove something
// is not a reason to skip the backup.
func SweepStaleTemps(ctx context.Context, cfg types.Config, store interfaces.Storer) {
	if store != nil {
		if err := store.Sweep(ctx); err != nil {
			log.Printf("[sweep] WARNING: could not sweep %s: %v", store.Location(), err)
		}
	}
	if cfg.ScratchDir != "" {
		sweepDir(cfg.ScratchDir, func(e os.DirEntry) bool {
			return e.IsDir() && strings.HasPrefix(e.Name(), scratchPrefix)
		})
	}
}

func sweepDir(dir string, match func(os.DirEntry) bool) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return // a missing directory has nothing stale in it
	}
	for _, e := range entries {
		if !match(e) {
			continue
		}
		path := filepath.Join(dir, e.Name())
		log.Printf("[sweep] removing leftover from a previous run: %s", path)
		if err := os.RemoveAll(path); err != nil {
			log.Printf("[sweep] WARNING: could not remove %s: %v", path, err)
		}
	}
}
