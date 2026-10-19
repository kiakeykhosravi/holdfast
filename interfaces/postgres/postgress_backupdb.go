package postgres

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"holdfast/types"

	"github.com/jackc/pgx/v5"
)

const maxParallelDumps = 3

type PostgresBackupDB struct {
	cfg types.Config
}

func NewPostgresBackupDB(cfg types.Config) *PostgresBackupDB {
	return &PostgresBackupDB{cfg: cfg}
}

func (p *PostgresBackupDB) Name() string { return "postgres" }

// ShouldRun is the preflight gate.
//
// Order matters here. The cluster-role check runs first, before anything
// type-specific, because whether this node should back up at all does not
// depend on what kind of backup was asked for. It used to run last: a node in
// the wrong role, asked for an incremental, hit the incremental preconditions
// first and returned a hard error — "summarize_wal is off" — which exits 1 and
// pages someone about a node that was never going to run, while the same node
// asked for a logical backup skipped cleanly with exit 0.
func (p *PostgresBackupDB) ShouldRun(ctx context.Context, req types.BackupRequest) (bool, string, error) {
	conn, err := pgx.Connect(ctx, p.cfg.PGConnString)
	if err != nil {
		return false, "", err
	}
	defer conn.Close(ctx)

	var inRecovery bool
	if err := conn.QueryRow(ctx, `SELECT pg_is_in_recovery();`).Scan(&inRecovery); err != nil {
		return false, "", err
	}

	ok, reason, err := p.checkClusterRole(ctx, conn, inRecovery)
	if err != nil || !ok {
		return ok, reason, err
	}

	if req.Type.Medium == types.MediumPhysical {
		if err := p.checkPhysicalPreconditions(ctx, conn, req); err != nil {
			return false, "", err
		}
	}
	if req.Type.RequiresParent() {
		if err := p.checkIncrementalPreconditions(ctx, conn, req); err != nil {
			return false, "", err
		}
	}
	return true, reason, nil
}

// checkClusterRole decides whether this node is the right one to back up.
func (p *PostgresBackupDB) checkClusterRole(ctx context.Context, conn *pgx.Conn, inRecovery bool) (bool, string, error) {
	switch strings.ToLower(strings.TrimSpace(p.cfg.RunOnClusterRole)) {
	case "replica":
		if !inRecovery {
			return false, "node is PRIMARY but RUN_ON_ROLE=replica", nil
		}

		var walStatus string
		if err := conn.QueryRow(ctx, `SELECT status FROM pg_stat_wal_receiver LIMIT 1;`).Scan(&walStatus); err != nil {
			if err == pgx.ErrNoRows {
				return false, "replica is in recovery but not receiving WAL (pg_stat_wal_receiver is empty)", nil
			}
			return false, "", err
		}
		if walStatus != "streaming" {
			return false, fmt.Sprintf("replica WAL receiver not streaming (status=%s)", walStatus), nil
		}

		var bytesBehind *int64
		if err := conn.QueryRow(ctx, `SELECT pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())::bigint;`).Scan(&bytesBehind); err != nil {
			return false, "", err
		}
		if bytesBehind == nil {
			return false, "replica WAL positions are not available yet (no WAL received/replayed)", nil
		}
		if *bytesBehind == 0 {
			return true, "replica OK (streaming; caught up)", nil
		}

		var lagSeconds *float64
		if err := conn.QueryRow(ctx, `SELECT EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))::float8;`).Scan(&lagSeconds); err != nil {
			return false, "", err
		}
		// A NULL replay timestamp means nothing has been replayed since this
		// replica started, so the lag is unbounded — not zero, and not the
		// 8-nanosecond sentinel the previous three overlapping branches left
		// behind when they happened to agree.
		if lagSeconds == nil {
			return false, fmt.Sprintf("replica has not replayed anything yet (pending replay %d bytes)", *bytesBehind), nil
		}
		lag := time.Duration(*lagSeconds * float64(time.Second))
		if lag < 0 {
			lag = 0 // clock skew between now() and the replay timestamp
		}
		if lag > p.cfg.MaxReplicaLag {
			return false, fmt.Sprintf("replica lag too high (pending replay %d bytes; %s > %s)", *bytesBehind, lag.Round(time.Second), p.cfg.MaxReplicaLag), nil
		}
		return true, fmt.Sprintf("replica OK (pending replay %d bytes; %s <= %s)", *bytesBehind, lag.Round(time.Second), p.cfg.MaxReplicaLag), nil

	case "primary":
		if inRecovery {
			return false, "node is in recovery but RUN_ON_ROLE=primary", nil
		}
		log.Println("WARNING: running backup on PRIMARY node — consider using a replica")
		return true, "primary OK (warning: backup on primary)", nil

	default:
		return false, fmt.Sprintf("invalid RunOnClusterRole=%q", p.cfg.RunOnClusterRole), nil
	}
}

// DumpToArchive dispatches on the requested type. Each medium gets its own
// method; nothing about the logical path changed when this switch was added.
func (p *PostgresBackupDB) DumpToArchive(ctx context.Context, zw *zip.Writer, req types.BackupRequest) (*types.DumpOutcome, error) {
	if err := p.ValidateRequest(req); err != nil {
		return nil, err
	}
	switch req.Type {
	case types.LogicalFull:
		var entries []string
		if err := p.dumpLogicalFull(ctx, zw, &entries); err != nil {
			return nil, err
		}
		return &types.DumpOutcome{Entries: entries}, nil
	case types.PhysicalFull, types.PhysicalIncremental:
		var entries []string
		return p.dumpPhysical(ctx, zw, req, &entries)
	default:
		// Unreachable while SupportedTypes gates the caller, but a missing
		// case here must not silently produce an empty archive.
		return nil, fmt.Errorf("postgres: no dump implementation for %s", req.Type)
	}
}

// dumpLogicalFull streams globals and every database into the zip writer. It
// appends each zip entry name to entries in the order written, which is restore
// order.
func (p *PostgresBackupDB) dumpLogicalFull(ctx context.Context, zw *zip.Writer, entries *[]string) error {
	// 1. Get the list of databases to backup
	conn, err := pgx.Connect(ctx, p.cfg.PGConnString)
	if err != nil {
		return fmt.Errorf("failed to connect to list databases: %w", err)
	}
	defer conn.Close(ctx)

	// datallowconn is checked as well as datistemplate: an administrator can
	// set datallowconn = false on an ordinary database, and pg_dump then fails
	// on it mid-run. Skipping it with a log line is better than failing the
	// whole backup over a database nobody can connect to anyway.
	rows, err := conn.Query(ctx, `SELECT datname FROM pg_database WHERE datistemplate = false AND datallowconn ORDER BY datname ASC;`)
	if err != nil {
		conn.Close(ctx)
		return err
	}
	defer rows.Close()

	excluded := excludeSet(p.cfg.ExcludeDBs)

	var dbs []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			conn.Close(ctx)
			return err
		}
		if excluded[name] {
			log.Printf("[dump] skipping  db=%s (excluded)", name)
			continue
		}
		// A database name becomes a zip entry name verbatim, and Postgres
		// permits "/" and ".." inside an identifier. Unzip rejects any entry
		// containing ".." or an absolute path, so such a name produces an
		// archive that cannot be extracted — the backup would appear to
		// succeed and then be unrestorable. Fail at dump time, loudly, in the
		// same spirit as the unsupported-tablespace path.
		if err := safeAsArchiveEntry(name); err != nil {
			rows.Close()
			conn.Close(ctx)
			return fmt.Errorf("database %q cannot be archived: %w", name, err)
		}
		dbs = append(dbs, name)
	}
	rows.Close()
	conn.Close(ctx) // done with the listing connection; dumps use their own pg_dump connections

	connNoPass, pgpassFile, env, err := p.buildPgToolEnv(os.TempDir())
	if err != nil {
		return fmt.Errorf("failed to setup pg environment: %w", err)
	}
	if pgpassFile != "" {
		defer os.Remove(pgpassFile)
	}

	globalsHeader := &zip.FileHeader{
		Name:     "globals.sql",
		Method:   zip.Deflate,
		Modified: time.Now(),
	}
	gw, err := zw.CreateHeader(globalsHeader)
	if err != nil {
		return err
	}
	*entries = append(*entries, globalsHeader.Name)

	// One deadline for the whole dump step. The per-database pg_dump calls
	// used to get their own DumpTimeout each, so a 30-database cluster could
	// legitimately run for 30 × dump_timeout. The config key reads as a bound
	// on the run; now it is one.
	cctx, cancel := withOptionalTimeout(ctx, p.cfg.DumpTimeout)
	defer cancel() // ensure resources are freed

	cmdGlobals := exec.CommandContext(cctx, p.cfg.PGDumpAllPath, "--globals-only", "--dbname="+p.connForDB(connNoPass, "postgres"))
	cmdGlobals.Env = env
	cmdGlobals.Stdout = gw // Pipe directly to zip

	if err := cmdGlobals.Run(); err != nil {
		return fmt.Errorf("streaming dump globals failed: %w", err)
	}

	if p.cfg.ScratchDir != "" {
		if err := os.MkdirAll(p.cfg.ScratchDir, 0o750); err != nil {
			return fmt.Errorf("failed to create scratch dir base %s: %w", p.cfg.ScratchDir, err)
		}
	}
	scratchDir, err := os.MkdirTemp(p.cfg.ScratchDir, "holdfast_dumps_")
	if err != nil {
		return fmt.Errorf("failed to create scratch dir: %w", err)
	}
	defer os.RemoveAll(scratchDir)

	type dumpJob struct {
		dbName  string
		tmpPath string
		size    int64
		err     error
	}
	jobs := make([]dumpJob, len(dbs))
	for i, name := range dbs {
		jobs[i] = dumpJob{dbName: name, tmpPath: filepath.Join(scratchDir, fmt.Sprintf("%d.dump", i))}
	}

	// Cancelling dumpCtx (on the first failure) stops every other in-flight
	// pg_dump too, instead of letting them run to completion pointlessly.
	dumpCtx, dumpCancel := context.WithCancel(cctx)
	defer dumpCancel()

	sem := make(chan struct{}, maxParallelDumps)
	var wg sync.WaitGroup
	var firstErrOnce sync.Once
	var firstErr error

	for i := range jobs {
		i := i
		wg.Add(1)
		sem <- struct{}{} // blocks here once maxParallelDumps workers are in flight
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			job := &jobs[i]
			f, ferr := os.OpenFile(job.tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
			if ferr != nil {
				job.err = fmt.Errorf("db %s: create scratch file: %w", job.dbName, ferr)
				firstErrOnce.Do(func() { firstErr = job.err; dumpCancel() })
				return
			}
			defer f.Close()

			cw := &countingWriter{w: f}

			cmdArgs := []string{"--dbname=" + p.connForDB(connNoPass, job.dbName), "-F", "c"}
			for _, t := range p.cfg.ExcludeTableData[job.dbName] {
				if t = strings.TrimSpace(t); t != "" {
					log.Printf("[dump] db=%s  excluding data for table=%s (schema kept)", job.dbName, t)
					cmdArgs = append(cmdArgs, "--exclude-table-data="+t)
				}
			}
			for _, t := range p.cfg.ExcludeTables[job.dbName] {
				if t = strings.TrimSpace(t); t != "" {
					log.Printf("[dump] db=%s  excluding table=%s (schema+data dropped)", job.dbName, t)
					cmdArgs = append(cmdArgs, "--exclude-table="+t)
				}
			}

			cmdDb := exec.CommandContext(dumpCtx, p.cfg.PGDumpPath, cmdArgs...)
			cmdDb.Env = env
			cmdDb.Stdout = cw

			dbStart := time.Now()
			log.Printf("[dump] starting  db=%s", job.dbName)
			if runErr := cmdDb.Run(); runErr != nil {
				job.err = fmt.Errorf("streaming dump failed for database %s: %w", job.dbName, runErr)
				firstErrOnce.Do(func() { firstErr = job.err; dumpCancel() })
				return
			}
			job.size = cw.n
			log.Printf("[dump] finished  db=%s  size=%s  elapsed=%s",
				job.dbName, formatBytes(job.size), time.Since(dbStart).Round(time.Second))
		}()
	}
	wg.Wait()

	if firstErr != nil {
		return firstErr
	}

	for _, job := range jobs {
		dbHeader := &zip.FileHeader{
			Name:     job.dbName + ".dump",
			Method:   zip.Store, // pg_dump -F c is already compressed
			Modified: time.Now(),
		}
		dw, err := zw.CreateHeader(dbHeader)
		if err != nil {
			return err
		}
		*entries = append(*entries, dbHeader.Name)
		src, err := os.Open(job.tmpPath)
		if err != nil {
			return fmt.Errorf("db %s: reopen scratch dump: %w", job.dbName, err)
		}
		_, copyErr := io.Copy(dw, src)
		src.Close()
		os.Remove(job.tmpPath) // free scratch space as we go, don't wait for the deferred RemoveAll
		if copyErr != nil {
			return fmt.Errorf("db %s: copy into archive: %w", job.dbName, copyErr)
		}
	}

	return nil
}

// ValidateChain proves the chain restores. A logical backup is self-contained,
// so its chain always has exactly one member and validation is the existing
// per-file restore into a throwaway container.
func (p *PostgresBackupDB) ValidateChain(ctx context.Context, chain []types.ExtractedBackup) error {
	if len(chain) == 0 {
		return fmt.Errorf("nothing to validate: empty chain")
	}
	if chain[0].Meta.Type.Medium == types.MediumPhysical {
		return p.validatePhysicalChain(ctx, chain)
	}
	if len(chain) != 1 {
		return fmt.Errorf("postgres: a logical backup chain must have exactly one member, got %d", len(chain))
	}
	return p.validateLogicalFiles(ctx, chain[0].Files)
}

func (p *PostgresBackupDB) validateLogicalFiles(parentCtx context.Context, dumpFiles []string) error {
	log.Println("Starting backup validation...")

	// restore_timeout used to apply only to the physical path. Here the
	// readiness loop and every pg_restore ran on the raw run context, so a
	// container that started but never became ready looped forever and a
	// huge restore ran unbounded.
	ctx, cancel := withOptionalTimeout(parentCtx, p.cfg.RestoreTimeout)
	defer cancel()

	containerName := fmt.Sprintf("holdfast_validate_%d", time.Now().UnixNano())

	var validationDataDir string
	if p.cfg.ValidationDataDir != "" {
		validationDataDir = filepath.Join(p.cfg.ValidationDataDir, containerName)
		if err := os.MkdirAll(validationDataDir, 0o777); err != nil {
			return fmt.Errorf("failed to create validation data dir %s: %w", validationDataDir, err)
		}
		if err := os.Chmod(validationDataDir, 0o777); err != nil {
			return fmt.Errorf("failed to chmod validation data dir %s: %w", validationDataDir, err)
		}
		defer os.RemoveAll(validationDataDir)
	}

	startArgs := []string{"run", "-d", "--rm", "--name", containerName, "-e", "POSTGRES_PASSWORD=testpass"}
	if validationDataDir != "" {
		startArgs = append(startArgs, "-v", validationDataDir+":/var/lib/postgresql")
	}
	startArgs = append(startArgs, p.cfg.PGTestDockerImage)
	startCmd := exec.CommandContext(ctx, "docker", startArgs...)
	if out, err := startCmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to start validation container: %w: %s", err, out)
	}
	defer func() {
		if out, err := exec.Command("docker", "rm", "-f", containerName).CombinedOutput(); err != nil {
			log.Printf("warning: failed to clean up validation container %s: %v: %s", containerName, err, out)
		}
	}()

	readyCmd := exec.CommandContext(ctx, "docker", "exec", containerName,
		"bash", "-c", "until pg_isready -U postgres >/dev/null 2>&1; do sleep 0.5; done")
	if out, err := readyCmd.CombinedOutput(); err != nil {
		logsOut, _ := exec.Command("docker", "logs", containerName).CombinedOutput()
		inspectOut, _ := exec.Command("docker", "inspect",
			"--format", "status={{.State.Status}} exitCode={{.State.ExitCode}} oomKilled={{.State.OOMKilled}} error={{.State.Error}}",
			containerName).CombinedOutput()
		return fmt.Errorf("postgres never became ready inside validation container: %w: %s\ncontainer state: %s\ncontainer logs:\n%s",
			err, out, inspectOut, logsOut)
	}
	for _, file := range dumpFiles {
		log.Printf("Validating file: %s", file)

		absPath, err := filepath.Abs(file)
		if err != nil {
			return fmt.Errorf("cannot resolve path %s: %w", file, err)
		}

		if out, err := exec.CommandContext(ctx, "docker", "cp", absPath, containerName+":/tmp/restore.tmp").CombinedOutput(); err != nil {
			return fmt.Errorf("failed to copy %s into validation container: %w: %s", file, err, out)
		}

		base := filepath.Base(file)
		isGlobals := strings.HasSuffix(base, ".sql")

		var restoreCmd *exec.Cmd
		if isGlobals {
			restoreCmd = exec.CommandContext(ctx, "docker", "exec", containerName,
				"psql", "-U", "postgres", "-f", "/tmp/restore.tmp")
		} else {
			dbName := strings.TrimSuffix(base, filepath.Ext(base))
			// Quote through pgx rather than wrapping in literal double quotes.
			// dbName originates from a zip entry name, so it is attacker-
			// influenced if anyone can place a file in the archive; a name
			// containing a double quote broke out of the old quoting and ran as
			// SQL inside the validation container.
			createCmd := exec.CommandContext(ctx, "docker", "exec", containerName,
				"psql", "-U", "postgres", "-c", "CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize())
			if out, err := createCmd.CombinedOutput(); err != nil {
				if !strings.Contains(string(out), "already exists") {
					return fmt.Errorf("failed to create validation database for %s: %w: %s", file, err, out)
				}
			}
			restoreCmd = exec.CommandContext(ctx, "docker", "exec", containerName,
				"pg_restore", "-U", "postgres", "--no-owner", "--no-acl", "-d", dbName, "/tmp/restore.tmp")
		}

		output, err := restoreCmd.CombinedOutput()
		if err != nil {
			log.Printf("Validation failed for %s: %s", file, string(output))
			return fmt.Errorf("validation failed for %s: %w%s\nlast output lines:\n%s",
				file, err, restoreHint(string(output)), lastLines(string(output), 15))
		}
		if isGlobals {
			if unexpected := unexpectedSQLErrors(string(output)); len(unexpected) > 0 {
				log.Printf("Validation failed for %s: %s", file, string(output))
				return fmt.Errorf("validation failed for %s: unexpected error(s): %s", file, strings.Join(unexpected, "; "))
			}
		}
		log.Printf("Validated %s OK", file)
	}
	return nil
}

// restoreHint names the usual reason a restore fails inside a stock image.
// pg_restore exits non-zero when any object could not be created, and the
// most common such object is an extension the image does not ship (PostGIS,
// TimescaleDB, pg_cron). The test is right to fail — the backup needs that
// extension to restore — but the operator needs to know it is the image, not
// the dump.
func restoreHint(output string) string {
	s := strings.ToLower(output)
	if strings.Contains(s, "extension") && (strings.Contains(s, "could not open extension control file") ||
		strings.Contains(s, "is not available") || strings.Contains(s, "no such file")) {
		return " (an extension used by this database is not installed in pg_test_docker_image; point it at an image that has it)"
	}
	return ""
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// withOptionalTimeout bounds ctx by d when d is positive. A zero duration
// means "no limit", which context.WithTimeout would turn into "already
// expired".
func withOptionalTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d > 0 {
		return context.WithTimeout(ctx, d)
	}
	return context.WithCancel(ctx)
}

// knownHarmlessGlobalsError matches the one error pg_dumpall's own docs say

var knownHarmlessGlobalsError = regexp.MustCompile(`(?i)role ".+" already exists`)

func unexpectedSQLErrors(output string) []string {
	var unexpected []string
	for _, line := range strings.Split(output, "\n") {
		if !strings.Contains(line, "ERROR:") || knownHarmlessGlobalsError.MatchString(line) {
			continue
		}
		unexpected = append(unexpected, strings.TrimSpace(line))
	}
	return unexpected
}
func (p *PostgresBackupDB) buildPgToolEnv(tmpDir string) (string, string, []string, error) {
	u, err := url.Parse(p.cfg.PGConnString)
	if err != nil {
		return "", "", nil, err
	}
	pass, _ := u.User.Password()
	u.User = url.User(u.User.Username())
	connNoPass := u.String()

	host, port := u.Hostname(), u.Port()
	if host == "" {
		// postgres:///db?host=/var/run/postgresql — a Unix socket. libpq
		// matches socket connections against a .pgpass host of "localhost".
		host = u.Query().Get("host")
	}
	if host == "" || strings.HasPrefix(host, "/") {
		host = "localhost"
	}
	if port == "" {
		port = u.Query().Get("port")
	}
	if port == "" {
		port = "5432"
	}

	// Create the .pgpass with O_EXCL via CreateTemp rather than a timestamp
	// name plus WriteFile. The old name was predictable, and WriteFile follows
	// symlinks: anyone able to create a path in the shared temp directory could
	// have pre-planted a link and had this process write the database password
	// through it, or truncate a file of their choosing.
	f, err := os.CreateTemp(tmpDir, ".pgpass_*")
	if err != nil {
		return "", "", nil, err
	}
	pgpassPath := f.Name()
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		os.Remove(pgpassPath)
		return "", "", nil, err
	}
	// Each .pgpass field must escape backslash and colon. Written verbatim, a
	// password containing ":" authenticated fine through pgx in preflight and
	// then failed in every pg_dump, which was baffling to debug.
	content := fmt.Sprintf("%s:%s:*:%s:%s\n", pgpassEscape(host), pgpassEscape(port), pgpassEscape(u.User.Username()), pgpassEscape(pass))
	if _, err := f.WriteString(content); err != nil {
		f.Close()
		os.Remove(pgpassPath)
		return "", "", nil, err
	}
	if err := f.Close(); err != nil {
		os.Remove(pgpassPath)
		return "", "", nil, err
	}

	env := append(os.Environ(), "PGPASSFILE="+pgpassPath)
	return connNoPass, pgpassPath, env, nil
}

// pgpassEscape escapes the two characters that are special in a .pgpass field.
func pgpassEscape(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, ":", `\:`)
}

func (p *PostgresBackupDB) connForDB(connStr, dbName string) string {
	u, _ := url.Parse(connStr)
	u.Path = "/" + dbName
	return u.String()
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

func excludeSet(names []string) map[string]bool {
	set := make(map[string]bool, len(names))
	for _, n := range names {
		if n = strings.TrimSpace(n); n != "" {
			set[n] = true
		}
	}
	return set
}

func formatBytes(b int64) string {
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

// safeAsArchiveEntry reports whether a database name can be used as a zip
// entry name without producing an archive that Unzip will refuse.
//
// Restore maps the entry name back to a database name by stripping ".dump", so
// escaping the name instead of rejecting it would need a matching unescape on
// the restore side and in every tool that inspects an archive by hand. Given
// how rare these names are, refusing is the honest trade.
func safeAsArchiveEntry(name string) error {
	switch {
	case name == "":
		return fmt.Errorf("name is empty")
	case strings.ContainsAny(name, `/\`):
		return fmt.Errorf("name contains a path separator, which would create a nested archive entry")
	case strings.Contains(name, ".."):
		return fmt.Errorf("name contains \"..\", which extraction refuses as a path-traversal attempt")
	case strings.HasPrefix(name, "."):
		return fmt.Errorf("name starts with a dot, which collides with the temp-file convention in the backup directory")
	case name == "globals.sql" || name == manifestName || strings.HasSuffix(name, ".tar"):
		return fmt.Errorf("name collides with the reserved archive entry %q", name)
	}
	return nil
}
