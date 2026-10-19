package api

import (
	"fmt"
	"syscall"
)

// FreeBytes reports the space available to an unprivileged writer on the
// filesystem holding path.
//
// Bavail is deliberate: Bfree includes blocks reserved for root, which this
// process cannot use, so reporting it would promise space that a dump cannot
// actually consume.
func FreeBytes(path string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, fmt.Errorf("statfs %s: %w", path, err)
	}
	return int64(st.Bavail) * int64(st.Bsize), nil
}

// RequireFreeBytes fails if path's filesystem has less than need bytes free.
//
// # Why this exists
//
// Running out of scratch mid-dump is the worst way to discover the problem:
// pg_basebackup dies partway through, a cycle's worth of load on the source
// cluster is wasted, and before the pipe-unblocking fix the process would hang
// rather than report it. Checking first turns hours of wasted I/O into an
// immediate, actionable error.
//
// The estimate is deliberately coarse — the point is to catch "this was never
// going to fit", not to predict within a percent.
func RequireFreeBytes(path string, need int64, what string) error {
	if need <= 0 {
		return nil
	}
	free, err := FreeBytes(path)
	if err != nil {
		// Not fatal: an unusual filesystem should not stop a backup that may
		// well have succeeded. The dump reports its own ENOSPC either way.
		return nil
	}
	if free < need {
		return fmt.Errorf("%s needs about %s of free space in %s but only %s is available",
			what, FormatBytes(need), path, FormatBytes(free))
	}
	return nil
}

// FormatBytes renders a byte count for human-facing messages.
func FormatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
