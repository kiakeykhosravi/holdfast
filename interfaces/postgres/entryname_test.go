package postgres

import "testing"

// TestSafeAsArchiveEntry covers the database names that would produce an
// archive Unzip refuses to extract — a backup that appears to succeed and is
// unrestorable, which is the worst outcome available.
func TestSafeAsArchiveEntry(t *testing.T) {
	ok := []string{"app", "app_prod", "My DB", "db-1", "ünïcode"}
	for _, name := range ok {
		if err := safeAsArchiveEntry(name); err != nil {
			t.Errorf("safeAsArchiveEntry(%q) = %v, want nil", name, err)
		}
	}

	bad := []string{
		"",            // empty
		"a/b",         // path separator -> nested entry
		`a\b`,         // ditto on the other separator
		"foo..bar",    // Unzip rejects any entry containing ".."
		"..",          // traversal
		".hidden",     // collides with the .tmp convention
		"globals.sql", // reserved entry
		"base.tar",    // reserved entry
		"backup_manifest",
	}
	for _, name := range bad {
		if err := safeAsArchiveEntry(name); err == nil {
			t.Errorf("safeAsArchiveEntry(%q) = nil, want an error", name)
		}
	}
}

// TestImageMajorVersion covers the tag shapes a real deployment uses.
func TestImageMajorVersion(t *testing.T) {
	cases := []struct {
		image string
		major int
		ok    bool
	}{
		{"postgres:18", 18, true},
		{"postgres:17.4", 17, true},
		{"postgres:18-alpine", 18, true},
		{"registry.example.com/mirror/postgres:16.2-bookworm", 16, true},
		{"postgres", 0, false},
		{"postgres:latest", 0, false},
		{"postgres:", 0, false},
	}
	for _, tc := range cases {
		major, ok := imageMajorVersion(tc.image)
		if ok != tc.ok || major != tc.major {
			t.Errorf("imageMajorVersion(%q) = (%d, %v), want (%d, %v)", tc.image, major, ok, tc.major, tc.ok)
		}
	}
}

// TestIsSummaryUnavailable pins the classification boundary. Treating a
// permissions error as recoverable would silently turn every future incremental
// into a full while reporting success.
func TestIsSummaryUnavailable(t *testing.T) {
	// Wording taken from src/backend/backup/basebackup_incremental.c.
	recoverable := []string{
		`pg_basebackup: error: could not initiate base backup: ERROR:  WAL summaries are required on timeline 1 from 0/3000028 to 0/5000060, but no summaries for that timeline and LSN range exist`,
		`ERROR:  WAL summaries are required on timeline 1 from 0/3000028 to 0/5000060, but the summaries for that timeline and LSN range are incomplete`,
		`ERROR:  timed out waiting for WAL summarization`,
		// The standby-without-restartpoint case the fallback exists for.
		`ERROR:  manifest requires WAL from final timeline 1 ending at 0/5000060, but this backup starts at 0/5000028`,
		`ERROR:  manifest requires WAL from initial timeline 1 starting at 0/3000028, but that timeline begins at 0/4000000`,
		`ERROR:  timeline 3 found in manifest, but not in this server's history`,
		`ERROR:  system identifier in backup manifest is 7291, but database system identifier is 7292`,
	}
	for _, s := range recoverable {
		if !isSummaryUnavailable(s) {
			t.Errorf("isSummaryUnavailable(%q) = false, want true", s)
		}
	}

	notRecoverable := []string{
		`pg_basebackup: error: could not open file "/var/lib/holdfast/state/x": Permission denied`,
		`pg_basebackup: error: could not read file "backup_manifest": Permission denied`,
		`pg_basebackup: error: connection to server failed`,
		`pg_basebackup: error: directory "/scratch/backup" exists but is not empty`,
	}
	for _, s := range notRecoverable {
		if isSummaryUnavailable(s) {
			t.Errorf("isSummaryUnavailable(%q) = true; a real fault must not be masked as recoverable", s)
		}
	}
}

// TestPgpassEscape: .pgpass treats ":" and "\" as structure, so a password
// containing either authenticated through pgx and then failed in pg_dump.
func TestPgpassEscape(t *testing.T) {
	cases := map[string]string{
		"plain":      "plain",
		"a:b":        `a\:b`,
		`a\b`:        `a\\b`,
		`x:\:y`:      `x\:\\\:y`,
		"host.local": "host.local",
	}
	for in, want := range cases {
		if got := pgpassEscape(in); got != want {
			t.Errorf("pgpassEscape(%q) = %q, want %q", in, got, want)
		}
	}
}
