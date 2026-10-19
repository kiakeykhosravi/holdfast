package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	"holdfast/api"
	"holdfast/interfaces"
	"holdfast/interfaces/crypto"
	"holdfast/interfaces/filestore"
	"holdfast/interfaces/mirrorstore"
	"holdfast/interfaces/postgres"
	"holdfast/interfaces/s3store"
	"holdfast/types"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	configPathFlag := flag.String("config", "", "Path to config file (YAML)")
	decryptInFlag := flag.String("decrypt", "", "Encrypted backup to decrypt: a local file path, or an archive name in the configured storage")
	decryptOutFlag := flag.String("out", "", "Output path for -decrypt (default: input name without .enc, in the current directory)")
	testOnlyFlag := flag.Bool("test-only", false, "Restore-test an existing encrypted backup and exit")
	testFileFlag := flag.String("file", "", "Archive name in the configured storage for -test-only (default: the latest)")
	retentionOnlyFlag := flag.Bool("retention-only", false, "Run retention cleanup only and exit")
	retentionDaysFlag := flag.Int("retention-days", -1, "Override retention_days for this run (>=0)")
	dryRunFlag := flag.Bool("dry-run", false, "Validate config and DB connectivity without writing anything")
	syncFlag := flag.Bool("sync-mirrors", false, "Copy everything in the primary storage to the configured mirrors and exit (run this where backup_dir is served, not on the database host)")
	pruneFlag := flag.Bool("prune", false, "With -sync-mirrors: also delete mirror objects the primary no longer has")
	sourceFlag := flag.String("source", "", "Run against one configured source by name (default: all enabled sources for -sync-mirrors; required for other modes when several sources are configured)")
	backupTypeFlag := flag.String("type", "", "Backup type for this run, e.g. logical-full, physical-incremental (default: config backup_type)")
	parentFlag := flag.String("parent", "", "Explicit parent backup ID for an incremental (default: newest usable chain member)")
	flag.Parse()

	configPath := *configPathFlag
	if configPath == "" {
		configPath = os.Getenv("CONFIG_PATH")
	}

	opts := types.CLIOptions{
		ConfigPath:    configPath,
		DecryptIn:     strings.TrimSpace(*decryptInFlag),
		DecryptOut:    strings.TrimSpace(*decryptOutFlag),
		TestOnly:      *testOnlyFlag,
		TestFile:      *testFileFlag,
		RetentionOnly: *retentionOnlyFlag,
		DryRun:        *dryRunFlag,
		SyncMirrors:   *syncFlag,
		Prune:         *pruneFlag,
		Source:        strings.TrimSpace(*sourceFlag),
		BackupType:    strings.TrimSpace(*backupTypeFlag),
		ParentID:      strings.TrimSpace(*parentFlag),
	}
	if *retentionDaysFlag >= 0 {
		v := *retentionDaysFlag
		opts.RetentionDaysOverride = &v
	}
	if err := opts.InferMode(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(2)
	}
	cfg, err := loadConfig(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(2)
	}
	if opts.RetentionDaysOverride != nil {
		cfg.RetentionDays = *opts.RetentionDaysOverride
	}
	if opts.BackupType != "" {
		bt, parseErr := types.ParseBackupType(opts.BackupType)
		if parseErr != nil {
			fmt.Fprintf(os.Stderr, "ERROR: -type: %v\n", parseErr)
			os.Exit(2)
		}
		cfg.BackupType = bt
	}

	// Dry-run is included: it validates the config a real backup would use, and
	// "the passphrase is actually set on this host" is one of the things it
	// exists to tell you before 3am. Without this it failed with the config
	// validator's EncryptPassphrase-too-short blob, which does not point at the
	// env var.
	// -sync-mirrors is excluded on purpose: it copies ciphertext and needs
	// the passphrase only to verify sidecar signatures before pruning, and a
	// sync host should not have to hold the backup passphrase at all.
	if opts.Mode == types.RunModeBackup || opts.Mode == types.RunModeDryRun {
		if strings.TrimSpace(os.Getenv("BACKUP_PASSPHRASE")) == "" {
			fmt.Fprintf(os.Stderr,
				"ERROR: BACKUP_PASSPHRASE env var is not set.\n"+
					"       Refusing to run with a passphrase sourced from the config file.\n"+
					"       Set BACKUP_PASSPHRASE in your environment or systemd unit.\n")
			os.Exit(2)
		}
	}

	// Every mode but sync works on exactly one source. Resolve it now so the
	// rest of main, and all of api, see a plain single-directory config.
	// Sync keeps the full list and iterates it below.
	var sources []types.SourceConfig
	if selected, serr := cfg.SelectSources(opts.Source); serr != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", serr)
		os.Exit(2)
	} else {
		sources = selected
	}
	if opts.Mode != types.RunModeSyncMirrors {
		if len(sources) != 1 {
			fmt.Fprintf(os.Stderr, "ERROR: %d sources are enabled; pick one with -source (%s)\n", len(sources), strings.Join(cfg.SourceNames(), ", "))
			os.Exit(2)
		}
		cfg = cfg.ForSource(sources[0])
	}

	if errs := cfg.ValidateForMode(opts); len(errs) > 0 {
		b, _ := json.MarshalIndent(errs, "", "  ")
		fmt.Fprintf(os.Stderr, "CONFIG INVALID:\n%s\n", string(b))
		os.Exit(2)
	}

	pg := postgres.NewPostgresBackupDB(cfg)
	encryption := crypto.NewAESEncryptor()

	// The store is built like the driver and the encryptor: here, once, from
	// config. api never learns which backend it got. A dry run keeps going
	// without one so it can report why it is missing.
	var store interfaces.Storer
	var storeErr error
	if opts.Mode != types.RunModeSyncMirrors {
		store, storeErr = newStorer(ctx, cfg)
	}
	if storeErr != nil && opts.Mode != types.RunModeDryRun {
		fmt.Fprintf(os.Stderr, "ERROR: storage: %v\n", storeErr)
		os.Exit(2)
	}

	// Whether the configured type is *supported* can only be asked once the
	// driver exists, which is why this is here and not in Config.Validate.
	if opts.Mode == types.RunModeBackup || opts.Mode == types.RunModeDryRun {
		if !pg.Supports(cfg.BackupType) {
			fmt.Fprintf(os.Stderr, "ERROR: %s cannot produce %s (supported: %s)\n",
				pg.Name(), cfg.BackupType, formatTypes(pg.SupportedTypes()))
			os.Exit(2)
		}
	}

	// Parent resolution deliberately does NOT happen here. It lives in
	// StartBackup so that every reason an incremental can be impossible reaches
	// the one place that knows how to downgrade it to a full; resolving it here
	// meant a chain at max_chain_length exited 2 with no backup taken.
	var res any
	var dry *api.DryRunResult
	syncFailed := false
	switch opts.Mode {
	case types.RunModeDecrypt:
		res, err = api.DecryptEncryptedBackup(ctx, store, encryption, opts.DecryptIn, opts.DecryptOut, cfg.EncryptPassphrase)
	case types.RunModeValidateOnly:
		var name string
		name, err = objectNameArg(cfg, opts.TestFile)
		if err == nil {
			res, err = api.ValidateEncryptedBackup(ctx, cfg, store, pg, encryption, cfg.EncryptPassphrase, name)
		}
	case types.RunModeRetentionOnly:
		res, err = api.RunRetentionOnly(ctx, cfg, store)
	case types.RunModeSyncMirrors:
		// One store set per source, each mirror scoped to that source's
		// prefix. A source whose stores cannot be built is passed through as
		// failed so the others still run and the result names it.
		var syncSources []api.SyncSource
		for _, src := range sources {
			scfg := cfg.ForSource(src)
			ss := api.SyncSource{Name: src.Name}
			st, berr := newStorer(ctx, scfg)
			if berr != nil {
				ss.Err = berr
			} else if ms, ok := st.(*mirrorstore.Store); ok {
				ss.Primary, ss.Mirrors = ms.Primary(), ms.Mirrors()
			} else {
				ss.Err = fmt.Errorf("no mirrors configured: set storage.mirrors (e.g. [s3]) on this host")
			}
			syncSources = append(syncSources, ss)
		}
		var sres *types.SyncResult
		sres, err = api.SyncSources(ctx, cfg, syncSources, api.SyncOptions{Prune: opts.Prune})
		res = sres
		if err == nil && sres.Failed > 0 {
			syncFailed = true
		}
	case types.RunModeDryRun:
		// The result carries the diagnosis even when the verdict is "broken",
		// so it is kept separately and printed on both paths below.
		dry, err = api.RunDryRun(ctx, cfg, opts, store, storeErr, pg, encryption)
		res = dry
	default:
		res, err = api.StartBackup(ctx, cfg, store, pg, encryption, cfg.BackupType, opts.ParentID)
	}

	// Retention runs as its own step, after the backup and outside its JSON,
	// and it runs whether or not the backup step succeeded. It used to be
	// skipped on any error, so a restore test that kept failing — the
	// validation container not starting, say — meant a cluster-sized full
	// every run and nothing ever deleted. The only outcomes that skip it are
	// a skip (nothing was written) and a cancellation (get out quickly).
	if opts.Mode == types.RunModeBackup && ctx.Err() == nil && !isSkip(err) {
		if _, rerr := api.RunRetention(ctx, cfg, store); rerr != nil {
			fmt.Fprintf(os.Stderr, "WARNING: retention cleanup did not run: %v\n", rerr)
		}
	}

	if err != nil {
		// A skip is the preflight gate working, not a failure. Exiting 1 here
		// is what used to page someone for a healthy lagging replica.
		var skip *api.SkipError
		if errors.As(err, &skip) {
			b, _ := json.MarshalIndent(api.SkipResult{
				Mode:      string(opts.Mode),
				Skipped:   true,
				Reason:    skip.Reason,
				CreatedAt: time.Now(),
			}, "", "  ")
			fmt.Println(string(b))
			os.Exit(0)
		}
		if ctx.Err() != nil {
			fmt.Fprintf(os.Stderr, "CANCELLED: %v\n", ctx.Err())
			os.Exit(130)
		}
		if dry != nil {
			// The whole point of a dry run is the diagnosis. Print it even
			// though the verdict is a failure; "config validation failed"
			// alone sent people back to the config with no idea which key.
			b, _ := json.MarshalIndent(dry, "", "  ")
			fmt.Println(string(b))
		}
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}

	b, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(b))

	// The run succeeded on the primary. If a mirror missed anything, say so
	// and exit 3: distinguishable from success (0) and from a failed backup
	// (1), so a schedule can page for "offsite copy is behind" without
	// treating the local backup as lost.
	if syncFailed {
		fmt.Fprintf(os.Stderr, "DEGRADED: some objects could not be synced; see \"failed\" above. The next run retries them.\n")
		os.Exit(3)
	}
	if ms, ok := store.(*mirrorstore.Store); ok && store != nil {
		if failures := ms.Failures(); len(failures) > 0 {
			fmt.Fprintf(os.Stderr, "DEGRADED: the primary copy is complete but %d mirror operation(s) failed:\n", len(failures))
			for _, f := range failures {
				fmt.Fprintf(os.Stderr, "  - %s\n", f)
			}
			os.Exit(3)
		}
	}
}

// newStorer builds the primary store and wraps it with any mirrors.
func newStorer(ctx context.Context, cfg types.Config) (interfaces.Storer, error) {
	primary, err := newStorerOfKind(ctx, cfg, cfg.PrimaryStorage())
	if err != nil {
		return nil, err
	}
	var mirrors []interfaces.Storer
	for _, kind := range cfg.Storage.Mirrors {
		m, err := newStorerOfKind(ctx, cfg, kind)
		if err != nil {
			return nil, fmt.Errorf("mirror %s: %w", kind, err)
		}
		mirrors = append(mirrors, m)
	}
	return mirrorstore.New(primary, mirrors...), nil
}

func newStorerOfKind(ctx context.Context, cfg types.Config, kind types.StorageKind) (interfaces.Storer, error) {
	switch kind {
	case types.StorageFile:
		return filestore.New(cfg.BackupDir)
	case types.StorageS3:
		s3cfg := cfg.Storage.S3
		return s3store.New(ctx, s3store.Options{
			Bucket:          s3cfg.Bucket,
			Prefix:          s3cfg.Prefix,
			Region:          s3cfg.Region,
			Endpoint:        s3cfg.Endpoint,
			UsePathStyle:    s3cfg.UsePathStyle,
			AccessKeyID:     s3cfg.AccessKeyID,
			SecretAccessKey: s3cfg.SecretAccessKey,
			LegacyCompat:    s3cfg.LegacyCompat,
			PartSize:        s3cfg.PartSize,
			Concurrency:     s3cfg.Concurrency,
		})
	default:
		return nil, fmt.Errorf("unknown storage kind %q", kind)
	}
}

// objectNameArg turns a -file argument into an object name. A bare name is
// used as is. A path is accepted only for the file backend and only if it
// points into backup_dir, because silently testing a different file than the
// one named would hide an operator mistake.
func objectNameArg(cfg types.Config, v string) (string, error) {
	v = strings.TrimSpace(v)
	if v == "" || !strings.ContainsAny(v, `/\`) {
		return v, nil
	}
	if cfg.PrimaryStorage() != types.StorageFile {
		return "", fmt.Errorf("-file %q: with %s as the primary storage pass the archive name, not a path", v, cfg.PrimaryStorage())
	}
	absFile, err := filepath.Abs(v)
	if err != nil {
		return "", err
	}
	absDir, err := filepath.Abs(cfg.BackupDir)
	if err != nil {
		return "", err
	}
	if filepath.Dir(absFile) != absDir {
		return "", fmt.Errorf("-file %q is not inside backup_dir %s; pass the archive name or move the file", v, cfg.BackupDir)
	}
	return filepath.Base(absFile), nil
}

func isSkip(err error) bool {
	var skip *api.SkipError
	return errors.As(err, &skip)
}

func formatTypes(ts []types.BackupType) string {
	parts := make([]string, 0, len(ts))
	for _, t := range ts {
		parts = append(parts, t.String())
	}
	return strings.Join(parts, ", ")
}

func loadConfig(path string) (types.Config, error) {
	cfg := types.Config{
		BackupDir:           "/var/backups/dbbackup",
		RetentionDays:       14,
		MinChainsKept:       1,
		RunOnClusterRole:    "replica",
		MaxReplicaLag:       60 * time.Second,
		PGDumpPath:          "pg_dump",
		PGDumpAllPath:       "pg_dumpall",
		PGTestDockerImage:   "postgres:18",
		PGBaseBackupPath:    "pg_basebackup",
		PGCombineBackupPath: "pg_combinebackup",
		FallbackToFull:      true,
		MaxChainLength:      7,
		BackupType:          types.DefaultBackupType,
		StateDir:            "/var/lib/holdfast",
		// Without a default, os.MkdirTemp("") falls back to /tmp, which is
		// tmpfs on most hosts — a physical backup stages a full copy of the
		// cluster in here and would try to do it in RAM.
		ScratchDir:        "/var/tmp/holdfast",
		DumpTimeout:       2 * time.Hour,
		RestoreTimeout:    30 * time.Minute,
		EnableRestoreTest: true,
	}

	// file (optional)
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return types.Config{}, fmt.Errorf("read config file: %w", err)
		}
		if err := yaml.Unmarshal(b, &cfg); err != nil {
			return types.Config{}, fmt.Errorf("parse config file: %w", err)
		}
	}

	// env overrides (good for secrets + emergency overrides)
	if v := strings.TrimSpace(os.Getenv("BACKUP_DIR")); v != "" {
		cfg.BackupDir = v
	}
	// Fail rather than silently keep the default. Quietly ignoring a typo in a
	// retention setting means either keeping backups far longer than intended,
	// or — worse, before this was chain-safe — deleting on a schedule nobody
	// configured. BACKUP_TYPE already failed loudly; this now matches.
	if v := strings.TrimSpace(os.Getenv("RETENTION_DAYS")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return types.Config{}, fmt.Errorf("RETENTION_DAYS=%q is not a number", v)
		}
		if n < 0 {
			return types.Config{}, fmt.Errorf("RETENTION_DAYS=%d cannot be negative", n)
		}
		cfg.RetentionDays = n
	}
	if v := strings.TrimSpace(os.Getenv("MIN_CHAINS_KEPT")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return types.Config{}, fmt.Errorf("MIN_CHAINS_KEPT=%q must be a non-negative number", v)
		}
		cfg.MinChainsKept = n
	}
	if v := strings.TrimSpace(os.Getenv("BACKUP_TYPE")); v != "" {
		bt, err := types.ParseBackupType(v)
		if err != nil {
			return types.Config{}, fmt.Errorf("BACKUP_TYPE: %w", err)
		}
		cfg.BackupType = bt
	}
	// Storage. The S3 secret has the same status as the DB password: env in
	// production, never the config file. Leaving both S3 keys empty hands the
	// decision to the AWS SDK's default chain (AWS_* variables, shared
	// config, instance role).
	if v := strings.TrimSpace(os.Getenv("BACKUP_STORAGE")); v != "" {
		cfg.Storage.Primary = types.StorageKind(strings.ToLower(v))
	}
	if v := strings.TrimSpace(os.Getenv("BACKUP_STORAGE_MIRRORS")); v != "" {
		cfg.Storage.Mirrors = nil
		for _, part := range strings.Split(v, ",") {
			if part = strings.ToLower(strings.TrimSpace(part)); part != "" {
				cfg.Storage.Mirrors = append(cfg.Storage.Mirrors, types.StorageKind(part))
			}
		}
	}
	if v := strings.TrimSpace(os.Getenv("S3_BUCKET")); v != "" {
		cfg.Storage.S3.Bucket = v
	}
	if v := strings.TrimSpace(os.Getenv("S3_PREFIX")); v != "" {
		cfg.Storage.S3.Prefix = v
	}
	if v := strings.TrimSpace(os.Getenv("S3_ENDPOINT")); v != "" {
		cfg.Storage.S3.Endpoint = v
	}
	if v := strings.TrimSpace(os.Getenv("S3_REGION")); v != "" {
		cfg.Storage.S3.Region = v
	} else if cfg.Storage.S3.Region == "" {
		// The AWS SDK's own variables, so a host already set up for the aws
		// CLI needs nothing extra.
		for _, k := range []string{"AWS_REGION", "AWS_DEFAULT_REGION"} {
			if v := strings.TrimSpace(os.Getenv(k)); v != "" {
				cfg.Storage.S3.Region = v
				break
			}
		}
	}
	if v := strings.TrimSpace(os.Getenv("S3_ACCESS_KEY_ID")); v != "" {
		cfg.Storage.S3.AccessKeyID = v
	}
	if v := strings.TrimSpace(os.Getenv("S3_SECRET_ACCESS_KEY")); v != "" {
		cfg.Storage.S3.SecretAccessKey = v
	}
	if v := strings.TrimSpace(os.Getenv("STATE_DIR")); v != "" {
		cfg.StateDir = v
	}
	if v := strings.TrimSpace(os.Getenv("RUN_ON_ROLE")); v != "" {
		cfg.RunOnClusterRole = v
	}
	if v := strings.TrimSpace(os.Getenv("MAX_REPLICA_LAG_SECONDS")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return types.Config{}, fmt.Errorf("MAX_REPLICA_LAG_SECONDS=%q must be a positive number of seconds", v)
		}
		cfg.MaxReplicaLag = time.Duration(n) * time.Second
	}
	if v := strings.TrimSpace(os.Getenv("SCRATCH_DIR")); v != "" {
		cfg.ScratchDir = v
	}
	if v := strings.TrimSpace(os.Getenv("PG_DUMPALL_PATH")); v != "" {
		cfg.PGDumpAllPath = v
	}
	if v := strings.TrimSpace(os.Getenv("ENABLE_RESTORE_TEST")); v != "" {
		cfg.EnableRestoreTest = strings.EqualFold(v, "1") || strings.EqualFold(v, "true") || strings.EqualFold(v, "yes")
	}

	// secrets: prefer env in prod
	if v := strings.TrimSpace(os.Getenv("PG_CONN")); v != "" {
		cfg.PGConnString = v
	}
	if v := strings.TrimSpace(os.Getenv("BACKUP_PASSPHRASE")); v != "" {
		cfg.EncryptPassphrase = v
	}

	return cfg, nil
}
