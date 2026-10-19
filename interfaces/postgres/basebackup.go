package postgres

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"holdfast/types"

	"github.com/jackc/pgx/v5"
)

// minIncrementalVersion is the server version that introduced --incremental.
const minIncrementalVersion = 170000

// manifestName is the file pg_basebackup writes alongside the tars. It is both
// the proof of what was backed up and the input to the next incremental.
const manifestName = "backup_manifest"

// backupManifest is the subset of backup_manifest we read. The Files array is
// deliberately not modelled — it is enormous and we never look at it.
type backupManifest struct {
	Version   int `json:"PostgreSQL-Backup-Manifest-Version"`
	WALRanges []struct {
		Timeline int    `json:"Timeline"`
		StartLSN string `json:"Start-LSN"`
		EndLSN   string `json:"End-LSN"`
	} `json:"WAL-Ranges"`
}

// dumpPhysical runs pg_basebackup in tar format into scratch, then streams the
// resulting files into the zip.
//
// Unlike the logical path there is no per-database parallelism to manage:
// pg_basebackup copies the whole cluster in one pass. Scratch must therefore
// hold a full copy of the cluster before anything reaches the archive.
func (p *PostgresBackupDB) dumpPhysical(
	ctx context.Context,
	zw *zip.Writer,
	req types.BackupRequest,
	entries *[]string,
) (*types.DumpOutcome, error) {

	if p.cfg.ScratchDir != "" {
		if err := os.MkdirAll(p.cfg.ScratchDir, 0o750); err != nil {
			return nil, fmt.Errorf("failed to create scratch dir base %s: %w", p.cfg.ScratchDir, err)
		}
	}
	scratch, err := os.MkdirTemp(p.cfg.ScratchDir, "holdfast_basebackup_")
	if err != nil {
		return nil, fmt.Errorf("failed to create scratch dir: %w", err)
	}
	defer os.RemoveAll(scratch)

	// pg_basebackup insists on an empty target, and MkdirTemp just created one.
	target := filepath.Join(scratch, "backup")

	connNoPass, pgpassFile, env, err := p.buildPgToolEnv(os.TempDir())
	if err != nil {
		return nil, fmt.Errorf("failed to setup pg environment: %w", err)
	}
	if pgpassFile != "" {
		defer os.Remove(pgpassFile)
	}

	args := []string{
		"--pgdata=" + target,
		"--format=tar",
		// Stream WAL alongside the backup so the archive is self-contained and
		// restorable without a separate WAL archive.
		"--wal-method=stream",
		// Don't wait for the next scheduled checkpoint. On a primary this also
		// guarantees the summaries needed by the *next* incremental exist.
		"--checkpoint=fast",
		"--no-password",
		"--dbname=" + p.connForDB(connNoPass, "postgres"),
	}
	if req.Parent != nil {
		args = append(args, "--incremental="+req.Parent.StatePath)
	}

	cctx, cancel := context.WithTimeout(ctx, p.cfg.DumpTimeout)
	defer cancel()

	cmd := exec.CommandContext(cctx, p.cfg.PGBaseBackupPath, args...)
	cmd.Env = env
	var stderr strings.Builder
	cmd.Stderr = &stderr

	// Physical backups copy the data directory block by block: they have no
	// concept of a database or a table, so every exclude in the config is
	// silently inert. Someone reusing one config file for both a logical job
	// and a physical one needs to know that the data they thought they were
	// excluding is in the archive.
	//
	// Warned here rather than in ValidateRequest, which is called twice per run
	// (once by StartBackup, once by DumpToArchive) and so logged it twice.
	// ValidateRequest is also the wrong place for a side effect.
	if n := len(p.cfg.ExcludeDBs) + len(p.cfg.ExcludeTables) + len(p.cfg.ExcludeTableData); n > 0 {
		log.Printf("WARNING: %s ignores exclude_dbs/exclude_tables/exclude_table_data (%d rule(s) configured); a physical backup always covers the entire cluster",
			req.Type, n)
	}

	start := time.Now()
	if req.Parent != nil {
		log.Printf("[basebackup] starting incremental against parent=%s", req.Parent.ID)
	} else {
		log.Printf("[basebackup] starting full")
	}

	if err := cmd.Run(); err != nil {
		msg := stderr.String()
		// The server rejects an incremental whose summaries have aged out. That
		// is recoverable by taking a full, so surface it as the typed error api
		// knows how to downgrade rather than a generic failure.
		if req.Parent != nil && isSummaryUnavailable(msg) {
			return nil, &types.IncrementalUnavailableError{
				Reason: strings.TrimSpace(firstLine(msg)),
			}
		}
		return nil, fmt.Errorf("pg_basebackup failed: %w: %s", err, msg)
	}
	log.Printf("[basebackup] finished in %s", time.Since(start).Round(time.Second))

	manifestBytes, err := os.ReadFile(filepath.Join(target, manifestName))
	if err != nil {
		return nil, fmt.Errorf("pg_basebackup produced no %s: %w", manifestName, err)
	}

	if err := archiveBaseBackupDir(zw, target, entries); err != nil {
		return nil, err
	}

	outcome := &types.DumpOutcome{
		Entries: *entries,
		State:   manifestBytes,
	}
	if start, end, timeline, ok := parseManifest(manifestBytes); ok {
		outcome.PositionStart = start
		outcome.PositionEnd = end
		outcome.Timeline = timeline
	}
	return outcome, nil
}

// archiveBaseBackupDir copies every file pg_basebackup produced into the zip.
//
// Entry order is restore order: the base tar, then any tablespace tars, then
// the streamed WAL, then the manifest.
func archiveBaseBackupDir(zw *zip.Writer, dir string, entries *[]string) error {
	found, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	names := make([]string, 0, len(found))
	for _, e := range found {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	// Tie-break by name: baseBackupOrder returns the same rank for every
	// tablespace tar, and sort.Slice is not stable, so their relative order was
	// nondeterministic between runs. Entry order is restore order and is
	// recorded in the sidecar's EntryCount, so it should not vary.
	sort.Slice(names, func(i, j int) bool {
		oi, oj := baseBackupOrder(names[i]), baseBackupOrder(names[j])
		if oi != oj {
			return oi < oj
		}
		return names[i] < names[j]
	})

	for _, name := range names {
		method := zip.Store // tars of already-compressed pages; Deflate buys little
		if name == manifestName {
			method = zip.Deflate // JSON, compresses well and can be large
		}
		w, err := zw.CreateHeader(&zip.FileHeader{
			Name:     name,
			Method:   method,
			Modified: time.Now(),
		})
		if err != nil {
			return err
		}
		src, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(w, src)
		src.Close()
		if copyErr != nil {
			return fmt.Errorf("copy %s into archive: %w", name, copyErr)
		}
		*entries = append(*entries, name)
	}
	return nil
}

func baseBackupOrder(name string) int {
	switch {
	case name == "base.tar":
		return 0
	case name == "pg_wal.tar":
		return 2
	case name == manifestName:
		return 3
	default:
		return 1 // tablespace tars, named <oid>.tar
	}
}

func parseManifestWALRange(b []byte) (start, end string, ok bool) {
	s, e, _, ok := parseManifest(b)
	return s, e, ok
}

// parseManifest extracts the WAL range and timeline from a backup_manifest.
//
// The timeline is taken from the last range: if a manifest somehow spans a
// switch, the backup ends on the later one, and that is what a subsequent
// incremental has to match.
func parseManifest(b []byte) (start, end string, timeline int, ok bool) {
	var m backupManifest
	if err := json.Unmarshal(b, &m); err != nil {
		return "", "", 0, false
	}
	if len(m.WALRanges) == 0 {
		return "", "", 0, false
	}
	last := m.WALRanges[len(m.WALRanges)-1]
	return m.WALRanges[0].StartLSN, last.EndLSN, last.Timeline, true
}

// isSummaryUnavailable reports whether pg_basebackup refused an incremental for
// a reason a full backup would resolve.
//
// This used to also match any stderr containing "manifest", which is far too
// broad: `could not open file ".../backup_manifest": Permission denied` is a
// misconfiguration, and classifying it as recoverable silently converted every
// future incremental into a full while reporting success. A backup tool that
// hides a permissions problem behind a working-but-wrong code path is worse
// than one that fails.
//
// The patterns are prefixes of the server's own errmsg strings in
// src/backend/backup/basebackup_incremental.c, which the client relays on
// stderr. The previous list was guessed and missed the one case the comments
// kept naming: a standby that has not produced a restartpoint since the
// parent backup, which the server reports as "manifest requires WAL from
// final timeline ... but this backup starts at ...". That went to exit 1 with
// no fallback.
//
//   - "WAL summaries are required on timeline ... but no summaries ... exist"
//   - "WAL summaries are required on timeline ... are incomplete"
//   - "timed out waiting for WAL summarization" / "WAL summarization is not progressing"
//   - "manifest requires WAL from {initial,continuation,final,non-final} timeline ..."
//   - "timeline N found in manifest, but not in this server's history"
//   - "... system identifier ..." (manifest from a different cluster)
func isSummaryUnavailable(stderr string) bool {
	s := strings.ToLower(stderr)
	for _, pat := range []string{
		"wal summar",
		"manifest requires wal from",
		"found in manifest, but not in this server's history",
		"system identifier",
	} {
		if strings.Contains(s, pat) {
			return true
		}
	}
	return false
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// checkPhysicalPreconditions covers what any physical backup needs. The level
// matters only for the scratch estimate.
func (p *PostgresBackupDB) checkPhysicalPreconditions(ctx context.Context, conn *pgx.Conn, req types.BackupRequest) error {
	// --wal-method=stream opens a second replication connection alongside the
	// one doing the base backup. With max_wal_senders=1 — or 1 remaining free —
	// pg_basebackup fails well into the copy with a message about replication
	// slots that does not name the setting. Checking costs one query.
	var maxSenders, usedSenders int
	if err := conn.QueryRow(ctx, `SELECT current_setting('max_wal_senders')::int;`).Scan(&maxSenders); err == nil {
		if err := conn.QueryRow(ctx, `SELECT count(*)::int FROM pg_stat_replication;`).Scan(&usedSenders); err == nil {
			if maxSenders-usedSenders < 2 {
				return fmt.Errorf("physical backup with --wal-method=stream needs 2 free WAL senders but max_wal_senders=%d with %d in use; raise max_wal_senders",
					maxSenders, usedSenders)
			}
		}
	}

	// The scratch directory has to hold the backup before any of it reaches
	// the archive. Discovering that mid-dump wastes a full cycle of load on
	// the source cluster.
	var clusterBytes int64
	if err := conn.QueryRow(ctx,
		`SELECT coalesce(sum(pg_database_size(datname)), 0)::bigint FROM pg_database WHERE datallowconn;`,
	).Scan(&clusterBytes); err == nil && clusterBytes > 0 {
		scratch := p.cfg.ScratchDir
		if scratch == "" {
			scratch = os.TempDir()
		}
		var need int64
		var what string
		if req.Type.RequiresParent() {
			// An incremental holds only the blocks changed since the parent,
			// so demanding room for the whole cluster refused incrementals
			// that would have fit — and as a hard error, not a fallback. The
			// change volume is unknowable up front; a fifth of the cluster is
			// a coarse floor that catches "this scratch disk is tiny" while
			// leaving a genuinely large delta to report its own ENOSPC.
			need = clusterBytes / 5
			what = fmt.Sprintf("an incremental backup of this %s cluster (estimated at a fifth of its size)", formatBytes(clusterBytes))
		} else {
			// 15% headroom: pg_basebackup also streams WAL, and the per-database
			// sum omits shared catalogs and free space map overhead.
			need = clusterBytes + clusterBytes/100*15
			what = fmt.Sprintf("a physical backup of this %s cluster", formatBytes(clusterBytes))
		}
		if err := requireFree(scratch, need, what); err != nil {
			return err
		}
	}

	// Tablespaces cannot be validated: rebuilding the chain for
	// pg_combinebackup needs --tablespace-mapping and a decision about where
	// each one lands, which nothing here models. The validation path fails
	// loudly on a tablespace tar, which is correct — but it did so after the
	// hours-long dump, marked the backup failed, and the next run fell back
	// to a full that failed the same way. Refuse before the work instead.
	if p.cfg.EnableRestoreTest {
		var tablespaces int
		if err := conn.QueryRow(ctx,
			`SELECT count(*)::int FROM pg_tablespace WHERE spcname NOT IN ('pg_default', 'pg_global');`,
		).Scan(&tablespaces); err == nil && tablespaces > 0 {
			return fmt.Errorf("cluster has %d user tablespace(s), which the physical restore test cannot rebuild; set enable_restore_test: false for physical backups of this cluster, or use logical backups", tablespaces)
		}
	}

	// Tool and image versions must match the server's major version. A
	// mismatch is not detectable until pg_combinebackup refuses the chain, or
	// the validation container refuses to start on a data directory written by
	// a different major ("database files are incompatible with server") — both
	// long after the expensive work is done.
	var versionNum int
	if err := conn.QueryRow(ctx, `SELECT current_setting('server_version_num')::int;`).Scan(&versionNum); err != nil {
		return err
	}
	serverMajor := versionNum / 10000
	if err := checkToolMajor(ctx, p.cfg.PGBaseBackupPath, serverMajor); err != nil {
		return err
	}
	if p.cfg.EnableRestoreTest {
		if imageMajor, ok := imageMajorVersion(p.cfg.PGTestDockerImage); ok && imageMajor != serverMajor {
			return fmt.Errorf("pg_test_docker_image %q is PostgreSQL %d but the server is %d; a physical backup can only be validated by a matching major version",
				p.cfg.PGTestDockerImage, imageMajor, serverMajor)
		}
	}
	return nil
}

// toolVersionRe matches the trailing version in `<tool> (PostgreSQL) 18.1`.
var toolVersionRe = regexp.MustCompile(`(\d+)(?:\.(\d+))?\s*$`)

// checkToolMajor confirms a client binary's major version matches the server's.
func checkToolMajor(ctx context.Context, toolPath string, serverMajor int) error {
	out, err := exec.CommandContext(ctx, toolPath, "--version").CombinedOutput()
	if err != nil {
		return fmt.Errorf("cannot run %s --version: %w", toolPath, err)
	}
	m := toolVersionRe.FindSubmatch([]byte(strings.TrimSpace(string(out))))
	if m == nil {
		return nil // unrecognised format; don't block on a parsing failure
	}
	major, convErr := strconv.Atoi(string(m[1]))
	if convErr != nil {
		return nil
	}
	if major != serverMajor {
		return fmt.Errorf("%s is PostgreSQL %d but the server is %d; install the matching postgresql-client package",
			filepath.Base(toolPath), major, serverMajor)
	}
	return nil
}

// imageMajorVersion extracts the major version from a docker image reference
// such as "postgres:18" or "registry.example/postgres:18.1-alpine".
func imageMajorVersion(image string) (int, bool) {
	i := strings.LastIndex(image, ":")
	if i < 0 || i == len(image)-1 {
		return 0, false // no tag, e.g. "postgres" — nothing to compare
	}
	tag := image[i+1:]
	digits := ""
	for _, r := range tag {
		if r < '0' || r > '9' {
			break
		}
		digits += string(r)
	}
	if digits == "" {
		return 0, false // "latest", "alpine", ...
	}
	n, err := strconv.Atoi(digits)
	if err != nil {
		return 0, false
	}
	return n, true
}

// checkIncrementalPreconditions verifies the server can serve an incremental
// right now.
//
// Anything recoverable by taking a full backup instead is reported as
// IncrementalUnavailableError; genuine misconfiguration is a plain error.
func (p *PostgresBackupDB) checkIncrementalPreconditions(ctx context.Context, conn *pgx.Conn, req types.BackupRequest) error {
	var versionNum int
	if err := conn.QueryRow(ctx, `SELECT current_setting('server_version_num')::int;`).Scan(&versionNum); err != nil {
		return err
	}
	if versionNum < minIncrementalVersion {
		return fmt.Errorf("server version %d does not support incremental backup (need %d or later)", versionNum, minIncrementalVersion)
	}
	if err := checkToolMajor(ctx, p.cfg.PGCombineBackupPath, versionNum/10000); err != nil {
		return err
	}

	// A chain may not span a timeline change. After a failover or a PITR the
	// history has forked, and pg_combinebackup across the fork yields a cluster
	// that is not any point in time the database ever actually occupied. Taking
	// a full on the new timeline is the correct response, so this is reported
	// as recoverable.
	if req.Parent != nil && req.Parent.Timeline > 0 {
		var timeline int
		if err := conn.QueryRow(ctx, `SELECT timeline_id::int FROM pg_control_checkpoint();`).Scan(&timeline); err == nil {
			if timeline != req.Parent.Timeline {
				return &types.IncrementalUnavailableError{
					Reason: fmt.Sprintf("cluster is on timeline %d but parent %s was taken on timeline %d, so the history has forked (failover or PITR)",
						timeline, req.Parent.ID, req.Parent.Timeline),
				}
			}
		}
	}

	var summarize string
	if err := conn.QueryRow(ctx, `SELECT current_setting('summarize_wal');`).Scan(&summarize); err != nil {
		return err
	}
	if summarize != "on" {
		// Not recoverable by retrying as a full: the *next* incremental will
		// fail identically until someone enables it. Surface it loudly.
		return fmt.Errorf("summarize_wal is %q; incremental backup requires it to be on (ALTER SYSTEM SET summarize_wal = on; SELECT pg_reload_conf())", summarize)
	}

	var inRecovery bool
	if err := conn.QueryRow(ctx, `SELECT pg_is_in_recovery();`).Scan(&inRecovery); err != nil {
		return err
	}
	if inRecovery {
		// Supported, but the docs warn it can fail when little has happened
		// since the last backup, because a standby only advances at a
		// restartpoint. Worth a warning line when it does fail.
		log.Println("[basebackup] taking an incremental from a standby; this can fail if no restartpoint has occurred since the parent backup")
	}

	// The summaries on disk must cover every LSN from the parent's start LSN to
	// this backup's start LSN. We can only check the lower bound here, and only
	// if we know the parent's start position.
	if req.Parent != nil {
		if err := p.checkSummaryCoverage(ctx, conn, req); err != nil {
			return err
		}
	}
	return nil
}

func (p *PostgresBackupDB) checkSummaryCoverage(ctx context.Context, conn *pgx.Conn, req types.BackupRequest) error {
	meta, err := os.ReadFile(req.Parent.StatePath)
	if err != nil {
		if os.IsNotExist(err) {
			// Expected on a cold cache: StartBackup runs this preflight before
			// MaterialiseParentState, so that a node about to be told "wrong
			// role" does not first decrypt a whole archive. -dry-run never
			// populates it at all. pg_basebackup makes the authoritative check
			// either way, and its refusal is converted to
			// IncrementalUnavailableError above.
			log.Printf("[basebackup] parent state not cached at %s; skipping the WAL summary coverage check", req.Parent.StatePath)
			return nil
		}
		return fmt.Errorf("cannot read parent state %s: %w", req.Parent.StatePath, err)
	}
	parentStart, _, ok := parseManifestWALRange(meta)
	if !ok || parentStart == "" {
		return nil // nothing to check against; let the server decide
	}

	var covered bool
	err = conn.QueryRow(ctx,
		`SELECT coalesce(min(start_lsn) <= $1::pg_lsn, false) FROM pg_available_wal_summaries();`,
		parentStart,
	).Scan(&covered)
	if err != nil {
		// Best effort. If the function is unavailable or its shape differs on
		// this version, fall through — pg_basebackup itself will reject the
		// backup with a precise message, which we convert to
		// IncrementalUnavailableError above.
		log.Printf("[basebackup] could not verify WAL summary coverage, deferring to the server: %v", err)
		return nil
	}
	if !covered {
		return &types.IncrementalUnavailableError{
			Reason: fmt.Sprintf("WAL summaries no longer cover parent %s (start LSN %s); check wal_summary_keep_time", req.Parent.ID, parentStart),
		}
	}
	return nil
}
