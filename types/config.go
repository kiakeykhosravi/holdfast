package types

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	minPasswordLength = 16
)

// StorageKind selects the Storer implementation that holds finished backups.
type StorageKind string

const (
	StorageFile StorageKind = "file"
	StorageS3   StorageKind = "s3"
)

// StorageConfig says where finished artefacts go. Only backup_dir is affected:
// state_dir and scratch_dir are local regardless of the kind.
type StorageConfig struct {
	// Primary is the authoritative store: "file" (the default, backup_dir on
	// a local or mounted path) or "s3" (a bucket, configured below). Reads,
	// parent selection and retention decisions come from it.
	Primary StorageKind `yaml:"primary"`

	// Mirrors also receive every object written and every deletion. A
	// mirror failing is reported, never fatal: a backup that exists on the
	// primary but not offsite beats no backup. The usual setting is
	// primary: file with mirrors: [s3].
	Mirrors []StorageKind `yaml:"mirrors"`

	S3 S3Config `yaml:"s3"`
}

// S3Config configures the S3 backend. Credentials left empty fall through to
// the AWS SDK's default chain (environment, shared config, instance role),
// which is what production should use; static keys are for S3-compatible
// stores and tests.
type S3Config struct {
	Bucket          string `yaml:"bucket"`
	Prefix          string `yaml:"prefix"`
	Region          string `yaml:"region"`
	Endpoint        string `yaml:"endpoint"`
	UsePathStyle    bool   `yaml:"use_path_style"`
	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
	// LegacyCompat disables the SDK's default request checksums for servers
	// that reject them.
	LegacyCompat bool  `yaml:"legacy_compat"`
	PartSize     int64 `yaml:"part_size_bytes"`
	Concurrency  int   `yaml:"concurrency"`
}

// SourceConfig is one backup set: a directory of backup_* objects and the
// prefix it maps to in a mirror. A host that serves several clusters' backup
// directories lists them here and runs one process over all of them.
type SourceConfig struct {
	// Name identifies the source in flags, results and logs, and is the
	// default mirror prefix. Letters, digits, ".", "_" and "-".
	Name      string `yaml:"name"`
	BackupDir string `yaml:"backup_dir"`
	// Prefix overrides the mirror prefix for this source; default Name. It is
	// appended to storage.s3.prefix when that is set.
	Prefix string `yaml:"prefix"`
	// Enabled defaults to true. A disabled source is skipped by every mode
	// and can still be selected explicitly with -source.
	Enabled *bool `yaml:"enabled"`
}

// IsEnabled reports whether the source takes part in an unselected run.
func (s SourceConfig) IsEnabled() bool { return s.Enabled == nil || *s.Enabled }

type Config struct {
	BackupDir           string              `yaml:"backup_dir"`
	Storage             StorageConfig       `yaml:"storage"`
	Sources             []SourceConfig      `yaml:"sources"`
	BackupType          BackupType          `yaml:"backup_type"`
	StateDir            string              `yaml:"state_dir"`
	MaxChainLength      int                 `yaml:"max_chain_length"`
	RetentionDays       int                 `yaml:"retention_days"`
	FallbackToFull      bool                `yaml:"fallback_to_full"`
	EncryptPassphrase   string              `yaml:"encrypt_passphrase"`
	RunOnClusterRole    string              `yaml:"run_on_cluster_role"`
	MaxReplicaLag       time.Duration       `yaml:"max_replica_lag"`
	PGConnString        string              `yaml:"pg_conn_string"`
	PGDumpPath          string              `yaml:"pg_dump_path"`
	PGDumpAllPath       string              `yaml:"pg_dumpall_path"`
	PGTestDockerImage   string              `yaml:"pg_test_docker_image"`
	PGBaseBackupPath    string              `yaml:"pg_basebackup_path"`
	PGCombineBackupPath string              `yaml:"pg_combinebackup_path"`
	DumpTimeout         time.Duration       `yaml:"dump_timeout"`
	RestoreTimeout      time.Duration       `yaml:"restore_timeout"`
	EnableRestoreTest   bool                `yaml:"enable_restore_test"`
	ExcludeDBs          []string            `yaml:"exclude_dbs"`
	ExcludeTableData    map[string][]string `yaml:"exclude_table_data"`
	ExcludeTables       map[string][]string `yaml:"exclude_tables"`
	ScratchDir          string              `yaml:"scratch_dir"`
	ValidationDataDir   string              `yaml:"validation_data_dir"`
}

// PrimaryStorage is the authoritative backend, defaulting to file.
func (c Config) PrimaryStorage() StorageKind {
	if c.Storage.Primary == "" {
		return StorageFile
	}
	return c.Storage.Primary
}

// UsesStorage reports whether kind is the primary or one of the mirrors.
func (c Config) UsesStorage(kind StorageKind) bool {
	if c.PrimaryStorage() == kind {
		return true
	}
	for _, m := range c.Storage.Mirrors {
		if m == kind {
			return true
		}
	}
	return false
}

// validateStorage checks whatever the chosen backends need. With S3 as the
// primary the lock and parent-state cache cannot fall back to backup_dir, so
// state_dir becomes mandatory.
func (c Config) validateStorage(errs map[string]string) {
	valid := func(k StorageKind) bool { return k == StorageFile || k == StorageS3 }
	if !valid(c.PrimaryStorage()) {
		errs["Storage.Primary"] = fmt.Sprintf("storage.primary must be %q or %q", StorageFile, StorageS3)
	}
	seen := map[StorageKind]bool{c.PrimaryStorage(): true}
	for _, m := range c.Storage.Mirrors {
		switch {
		case !valid(m):
			errs["Storage.Mirrors"] = fmt.Sprintf("storage.mirrors entries must be %q or %q", StorageFile, StorageS3)
		case seen[m]:
			errs["Storage.Mirrors"] = fmt.Sprintf("storage.mirrors lists %q, which is already the primary or another mirror", m)
		}
		seen[m] = true
	}
	if c.UsesStorage(StorageFile) && len(c.Sources) == 0 && strings.TrimSpace(c.BackupDir) == "" {
		errs["BackupDir"] = "BackupDir is required when file storage is used"
	}
	if len(c.Sources) > 0 {
		c.validateSources(errs)
	}
	if c.UsesStorage(StorageS3) {
		if strings.TrimSpace(c.Storage.S3.Bucket) == "" {
			errs["Storage.S3.Bucket"] = "storage.s3.bucket is required when s3 storage is used"
		}
		if (strings.TrimSpace(c.Storage.S3.AccessKeyID) == "") != (strings.TrimSpace(c.Storage.S3.SecretAccessKey) == "") {
			errs["Storage.S3.Credentials"] = "storage.s3.access_key_id and secret_access_key must be set together, or both left empty for the SDK default chain"
		}
		// Without an endpoint this is real AWS, where the region has to be
		// the bucket's; a wrong or missing one fails every request. With an
		// endpoint the store defaults it, since compatible servers accept any.
		if strings.TrimSpace(c.Storage.S3.Endpoint) == "" && strings.TrimSpace(c.Storage.S3.Region) == "" {
			errs["Storage.S3.Region"] = "storage.s3.region is required for AWS S3 (or set AWS_REGION); it is optional only with an S3-compatible endpoint"
		}
	}
	if c.PrimaryStorage() == StorageS3 && strings.TrimSpace(c.StateDir) == "" {
		errs["StateDir"] = "state_dir is required with s3 as the primary storage: the run lock and parent-state cache need a local directory"
	}
}

var sourceNameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// validateSources enforces what makes several sources safe to run from one
// process: distinct names, distinct directories, and — the one that matters
// for prune — distinct effective mirror prefixes, so no source can ever
// delete another's objects.
func (c Config) validateSources(errs map[string]string) {
	if c.PrimaryStorage() != StorageFile {
		errs["Sources"] = "sources require storage.primary: file (each source is a directory)"
	}
	if strings.TrimSpace(c.StateDir) == "" {
		errs["StateDir"] = "state_dir is required when sources are configured: the run lock cannot fall back to a single backup_dir"
	}
	names := map[string]bool{}
	dirs := map[string]bool{}
	prefixes := map[string]string{}
	for i, src := range c.Sources {
		key := fmt.Sprintf("Sources[%d]", i)
		name := strings.TrimSpace(src.Name)
		switch {
		case name == "":
			errs[key+".Name"] = "name is required"
		case !sourceNameRe.MatchString(name):
			errs[key+".Name"] = fmt.Sprintf("name %q may only contain letters, digits, '.', '_' and '-'", name)
		case names[name]:
			errs[key+".Name"] = fmt.Sprintf("name %q is used twice", name)
		}
		names[name] = true

		dir := strings.TrimSpace(src.BackupDir)
		switch {
		case dir == "":
			errs[key+".BackupDir"] = "backup_dir is required"
		case dirs[dir]:
			errs[key+".BackupDir"] = fmt.Sprintf("backup_dir %q is used twice", dir)
		}
		dirs[dir] = true

		if c.UsesStorage(StorageS3) {
			p := c.effectivePrefix(src)
			if other, dup := prefixes[p]; dup {
				errs[key+".Prefix"] = fmt.Sprintf("effective prefix %q collides with source %q; prune would delete each other's objects", p, other)
			}
			prefixes[p] = name
		}
	}
}

// effectivePrefix is the mirror prefix a source's objects land under:
// storage.s3.prefix, then the source's own prefix or name.
func (c Config) effectivePrefix(src SourceConfig) string {
	part := strings.Trim(strings.TrimSpace(src.Prefix), "/")
	if part == "" {
		part = strings.TrimSpace(src.Name)
	}
	base := strings.Trim(strings.TrimSpace(c.Storage.S3.Prefix), "/")
	switch {
	case base == "":
		return part
	case part == "":
		return base
	default:
		return base + "/" + part
	}
}

// SelectSources returns the sources a run should cover. With name set it is
// that one source, enabled or not; otherwise every enabled source; with no
// sources configured, the implicit single source made of backup_dir and
// storage.s3.prefix, which is every config written before sources existed.
func (c Config) SelectSources(name string) ([]SourceConfig, error) {
	name = strings.TrimSpace(name)
	if len(c.Sources) == 0 {
		if name != "" {
			return nil, fmt.Errorf("-source %q: no sources are configured", name)
		}
		return []SourceConfig{{BackupDir: c.BackupDir, Prefix: c.Storage.S3.Prefix}}, nil
	}
	if name != "" {
		for _, s := range c.Sources {
			if s.Name == name {
				return []SourceConfig{s}, nil
			}
		}
		return nil, fmt.Errorf("-source %q: not a configured source (have %s)", name, strings.Join(c.SourceNames(), ", "))
	}
	var out []SourceConfig
	for _, s := range c.Sources {
		if s.IsEnabled() {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("every configured source is disabled; pick one with -source")
	}
	return out, nil
}

// SourceNames lists configured source names, for messages.
func (c Config) SourceNames() []string {
	out := make([]string, 0, len(c.Sources))
	for _, s := range c.Sources {
		out = append(out, s.Name)
	}
	return out
}

// ForSource returns the config as a single-source config: backup_dir and the
// mirror prefix resolved to this source. Every mode but sync runs on exactly
// one source, and this is how it gets there without knowing about sources.
func (c Config) ForSource(src SourceConfig) Config {
	out := c
	out.BackupDir = src.BackupDir
	if len(c.Sources) == 0 {
		return out // implicit source: prefix is already storage.s3.prefix
	}
	out.Storage.S3.Prefix = c.effectivePrefix(src)
	out.Sources = nil
	return out
}

func (c Config) Validate() map[string]string {
	errs := map[string]string{}
	c.validateStorage(errs)
	if c.RetentionDays < 0 {
		errs["RetentionDays"] = "RetentionDays cannot be less than 0"
	}
	if err := c.BackupType.Validate(); err != nil {
		errs["BackupType"] = err.Error()
	}
	if len(strings.TrimSpace(c.EncryptPassphrase)) < minPasswordLength {
		errs["EncryptPassphrase"] = fmt.Sprintf("EncryptPassphrase too short( min %d characters)", minPasswordLength)
	}
	role := strings.ToLower(strings.TrimSpace(c.RunOnClusterRole))
	if role != "replica" && role != "primary" {
		errs["RunOnClusterRole"] = `RunOnClusterRole must be "replica" or "primary"`
	}
	if c.MaxReplicaLag < 0 {
		errs["MaxReplicaLag"] = "MaxReplicaLag cannot be less than 0"
	}
	if strings.TrimSpace(c.PGConnString) == "" {
		errs["PGConnString"] = "PGConnString is required"
	} else if u, err := url.Parse(c.PGConnString); err != nil {
		errs["PGConnString"] = "Invalid PGConnString: " + err.Error()
	} else {
		if u.Scheme != "postgres" && u.Scheme != "postgresql" {
			errs["PGConnString"] = `PGConnString must be "postgres" or "postgresql"`
		}
		// A Unix-socket connection string has an empty authority and names the
		// socket directory in the query: postgres:///db?host=/var/run/postgresql
		if u.Host == "" && strings.TrimSpace(u.Query().Get("host")) == "" {
			errs["PGConnString"] = "PGConnString must have a non-empty host, or a host= query parameter naming a Unix socket directory"
		}
	}
	if strings.TrimSpace(c.PGDumpPath) == "" {
		errs["PGDumpPath"] = "PGDumpPath is required"
	}
	if strings.TrimSpace(c.PGDumpAllPath) == "" {
		errs["PGDumpAllPath"] = "PGDumpAllPath is required"
	}
	if strings.TrimSpace(c.PGTestDockerImage) == "" {
		errs["PGTestDockerImage"] = "PGTestDockerImage is required"
	}
	if c.DumpTimeout < 0 {
		errs["DumpTimeout"] = "DumpTimeout cannot be less than 0"
	}
	if c.RestoreTimeout < 0 {
		errs["RestoreTimeout"] = "RestoreTimeout cannot be less than 0"
	}
	return errs
}
