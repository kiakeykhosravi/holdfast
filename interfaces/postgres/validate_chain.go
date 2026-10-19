package postgres

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"holdfast/types"
)

// validatePhysicalChain proves a physical chain restores.
//
// This is the only operation that actually exercises the incremental machinery
// end to end: pg_combinebackup will refuse a chain with a missing or corrupt
// member, and a cluster that will not start is a backup that would not have
// restored.
func (p *PostgresBackupDB) validatePhysicalChain(ctx context.Context, chain []types.ExtractedBackup) error {
	work, err := os.MkdirTemp(p.cfg.ScratchDir, "holdfast_combine_")
	if err != nil {
		return fmt.Errorf("failed to create combine workdir: %w", err)
	}
	defer os.RemoveAll(work)

	// pg_combinebackup takes backup *directories*, but we archived tars. Undo
	// that for each member, oldest first.
	inputs := make([]string, 0, len(chain))
	for _, member := range chain {
		dir := filepath.Join(work, "in", member.Meta.ID)
		if err := rebuildBackupDir(ctx, member, dir); err != nil {
			return fmt.Errorf("member %s: %w", member.Meta.ID, err)
		}
		inputs = append(inputs, dir)
	}

	// pg_combinebackup writes one complete cluster, so the estimate is the
	// LARGEST input tree plus headroom — not the sum of them.
	//
	// Using the sum would overestimate by a factor of the chain length, and an
	// over-conservative check here is actively harmful: refusing to validate a
	// backup that would have fit marks it failed, which stops the next
	// incremental from chaining onto it. That is the failure this check exists
	// to prevent, arrived at from the other direction.
	var need int64
	for _, in := range inputs {
		if sz, err := dirSize(in); err == nil && sz > need {
			need = sz
		}
	}
	if need > 0 {
		if serr := requireFree(work, need+need/10, "pg_combinebackup output"); serr != nil {
			return serr
		}
	}

	target := filepath.Join(work, "combined")
	args := append(append([]string{}, inputs...), "-o", target)

	cctx, cancel := withOptionalTimeout(ctx, p.cfg.RestoreTimeout)
	defer cancel()

	log.Printf("[validate] combining %d backup(s) into %s", len(inputs), target)
	combine := exec.CommandContext(cctx, p.cfg.PGCombineBackupPath, args...)
	if out, err := combine.CombinedOutput(); err != nil {
		return fmt.Errorf("pg_combinebackup failed: %w: %s", err, out)
	}

	return p.bootAndCheck(cctx, target)
}

// rebuildBackupDir turns the archived tars back into the directory layout
// pg_combinebackup expects: base.tar unpacked at the root, pg_wal.tar into
// pg_wal/, and the manifest alongside them.
func rebuildBackupDir(ctx context.Context, member types.ExtractedBackup, dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}

	// Each tar is removed as soon as it has been unpacked. Peak scratch usage
	// is the binding constraint on how long a chain can be: the archive's tars
	// and the unpacked tree are both cluster-sized, so keeping both for every
	// member at once needed roughly 2N copies plus pg_combinebackup's output.
	// Releasing each tar immediately brings the peak down to about N+2.
	var sawBase bool
	for _, f := range member.Files {
		name := filepath.Base(f)
		switch {
		case name == "base.tar":
			if err := untar(ctx, f, dir); err != nil {
				return err
			}
			os.Remove(f)
			sawBase = true
		case name == "pg_wal.tar":
			walDir := filepath.Join(dir, "pg_wal")
			if err := os.MkdirAll(walDir, 0o700); err != nil {
				return err
			}
			if err := untar(ctx, f, walDir); err != nil {
				return err
			}
			os.Remove(f)
		case name == manifestName:
			b, err := os.ReadFile(f)
			if err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(dir, manifestName), b, 0o600); err != nil {
				return err
			}
		case strings.HasSuffix(name, ".tar"):
			// A tablespace tar. Restoring these needs
			// pg_combinebackup --tablespace-mapping and a decision about where
			// they land, which nothing in this project models yet. Failing is
			// correct: silently validating an incomplete cluster would be worse.
			return fmt.Errorf("archive contains tablespace tar %q; tablespaces are not supported by this validation path yet", name)
		}
	}
	if !sawBase {
		return fmt.Errorf("no base.tar in archive")
	}
	return nil
}

func untar(ctx context.Context, tarPath, dest string) error {
	cmd := exec.CommandContext(ctx, "tar", "-xf", tarPath, "-C", dest)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("untar %s: %w: %s", filepath.Base(tarPath), err, out)
	}
	return nil
}

// validationHBA is the pg_hba.conf the restored cluster boots with. The
// container is reachable only from this process, so trust is appropriate;
// what matters is that the sanity query can connect at all, which the source
// cluster's own pg_hba.conf (peer or scram for local connections) usually
// prevents.
const validationHBA = "local all all trust\nhost all all 127.0.0.1/32 trust\nhost all all ::1/128 trust\n"

const validationHBAName = "holdfast_validation_hba.conf"

// bootAndCheck starts a container on the combined data directory and confirms
// the cluster comes up and answers queries.
//
// # Why it overrides the cluster's configuration
//
// The combined directory contains the SOURCE cluster's postgresql.conf,
// postgresql.auto.conf and pg_hba.conf. Booted as-is, a production
// configuration fails in a throwaway container for reasons that have nothing
// to do with the backup: shared_preload_libraries naming extensions the image
// lacks, shared_buffers sized for the real host, ssl = on with certificates
// that are not there, huge_pages = on, a pg_hba.conf that will not let a
// socket connection in without a password nobody knows. Worse than failing,
// archive_mode = on would run the production archive_command from inside the
// container — and since crash recovery continues on the same timeline, the
// segments it archives can collide with the real archive's.
//
// Every one of those is overridden on the command line, which takes
// precedence over both configuration files. The restored data is untouched;
// only how the server is started changes.
func (p *PostgresBackupDB) bootAndCheck(ctx context.Context, dataDir string) error {
	// Postgres refuses to start on a data directory it does not own or that is
	// group/world accessible. The container therefore runs as *our* uid rather
	// than the image's postgres user — the 0777 trick the logical path uses
	// would make the cluster refuse to start.
	if err := os.Chmod(dataDir, 0o700); err != nil {
		return fmt.Errorf("chmod combined data dir: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, validationHBAName), []byte(validationHBA), 0o600); err != nil {
		return fmt.Errorf("write validation pg_hba.conf: %w", err)
	}
	absData, err := filepath.Abs(dataDir)
	if err != nil {
		return err
	}

	// The role and database to connect as come from the backup connection
	// string: the restored cluster IS the source cluster, so that role is the
	// one we know exists. "postgres" is only a guess at the superuser's name,
	// and clusters initialised under another name made the sanity query fail
	// on a perfectly good restore.
	role, db := "postgres", "postgres"
	if u, uerr := url.Parse(p.cfg.PGConnString); uerr == nil {
		if n := u.User.Username(); n != "" {
			role = n
		}
		if d := strings.TrimPrefix(u.Path, "/"); d != "" {
			db = d
		}
	}

	const containerData = "/var/lib/postgresql/data"
	containerName := fmt.Sprintf("holdfast_combine_validate_%d", time.Now().UnixNano())
	args := []string{
		"run", "-d", "--rm",
		"--name", containerName,
		"--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
		"-v", absData + ":" + containerData,
		"-e", "PGDATA=" + containerData,
		p.cfg.PGTestDockerImage,
		// Everything after the image is the server command line. -c settings
		// override postgresql.conf and postgresql.auto.conf.
		"postgres",
		"-c", "hba_file=" + containerData + "/" + validationHBAName,
		"-c", "archive_mode=off",
		"-c", "restore_command=",
		"-c", "recovery_end_command=",
		"-c", "primary_conninfo=",
		"-c", "primary_slot_name=",
		"-c", "synchronous_standby_names=",
		"-c", "shared_preload_libraries=",
		"-c", "ssl=off",
		"-c", "listen_addresses=localhost",
		"-c", "port=5432",
		"-c", "unix_socket_directories=/var/run/postgresql",
		"-c", "shared_buffers=128MB",
		"-c", "huge_pages=off",
		"-c", "logging_collector=off",
		"-c", "log_destination=stderr",
	}
	if out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err != nil {
		return fmt.Errorf("failed to start validation container: %w: %s", err, out)
	}
	defer func() {
		if out, err := exec.Command("docker", "rm", "-f", containerName).CombinedOutput(); err != nil {
			log.Printf("warning: failed to clean up validation container %s: %v: %s", containerName, err, out)
		}
	}()

	ready := exec.CommandContext(ctx, "docker", "exec", containerName,
		"bash", "-c", "until pg_isready -U "+role+" >/dev/null 2>&1; do sleep 0.5; done")
	if out, err := ready.CombinedOutput(); err != nil {
		logsOut, _ := exec.Command("docker", "logs", containerName).CombinedOutput()
		return fmt.Errorf("restored cluster never became ready: %w: %s\ncontainer logs:\n%s", err, out, logsOut)
	}

	// A cluster that starts but has no databases is not a restored cluster.
	check := exec.CommandContext(ctx, "docker", "exec", containerName,
		"psql", "-U", role, "-d", db, "-tAc",
		"SELECT count(*) FROM pg_database WHERE datistemplate = false;")
	out, err := check.CombinedOutput()
	if err != nil {
		return fmt.Errorf("restored cluster failed sanity query: %w: %s", err, out)
	}
	if strings.TrimSpace(string(out)) == "0" {
		return fmt.Errorf("restored cluster started but contains no databases")
	}

	log.Printf("[validate] restored cluster is up with %s database(s)", strings.TrimSpace(string(out)))
	return nil
}

// dirSize totals the apparent size of a directory tree.
func dirSize(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total, err
}

// requireFree fails if path's filesystem has less than need bytes available.
//
// Duplicated from api rather than imported: interfaces must not depend on api,
// which is the rule that keeps the dependency direction one-way.
func requireFree(path string, need int64, what string) error {
	if need <= 0 {
		return nil
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return nil // unusual filesystem; let the operation report its own error
	}
	free := int64(st.Bavail) * int64(st.Bsize)
	if free < need {
		return fmt.Errorf("%s needs about %s free in %s but only %s is available",
			what, formatBytes(need), path, formatBytes(free))
	}
	return nil
}
